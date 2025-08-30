#include <iostream>
#include <filesystem>
#include <vector>
#include <string>
#include <atomic>
#include <fstream>
#include <thread>
#include <cstdint>
#include <liburing.h>
#include <cstring>
#include <unistd.h>
#include <fcntl.h>
#include <mutex>
#include <sys/types.h>
#include <sys/sysinfo.h>
#if defined(__linux__)
  #include <sys/random.h>
  #include <sys/stat.h>
#endif
#include <array>
#include "xsimd/xsimd.hpp"
using batch_u8=xsimd::batch<uint8_t>;
constexpr size_t simd_u8_size=batch_u8::size;
using namespace std;
constexpr array<array<uint8_t,8>,256> make_rotl_table() {
    array<array<uint8_t,8>,256> table{};
    for (int b=0;b<256;++b) {
        for (int n=0;n<8;++n) {
            table[b][n]=static_cast<uint8_t>((b<<n)|(b>>(8-n)));
        }
    }
    return table;
}
constexpr auto rotl_table=make_rotl_table();
int fill_random(uint8_t* buf,size_t len) {
    if (!buf && len!=0) {
        return -1;
    }
    size_t offset=0;
    #if defined(__linux__)
    while (offset<len) {
        ssize_t r=getrandom(buf+offset,len-offset,0);
        if (r<0) {
            if (errno==EINTR) continue;
            if (errno==ENOSYS) break;
            return -1;
        }
        offset+=(size_t)r;
    }
    if (offset==len) return 0;
    #endif
    int fd=open("/dev/urandom",O_RDONLY | O_CLOEXEC);
    if (fd<0) {
        return -1;
    }
    while (offset<len) {
        ssize_t r=read(fd,buf+offset,len-offset);
        if (r<0) {
            if (errno==EINTR) continue;
            close(fd);
            return -1;
        }
        if (r==0) {
            close(fd);
            return -1;
        }
        offset+=(size_t)r;
    }
    close(fd);
    return 0;
}
#define IN_ITER 24
#define OUT_ITER 3
vector<uint8_t> entropy_buffer;
vector<uint8_t> small_entropy_buffer(IN_ITER);
vector<batch_u8> small_entropy_batches;
array<uint8_t,IN_ITER> modulo8_iter=[](){array<uint8_t,IN_ITER> out;for (int i=0;i<IN_ITER;i++) out[i]=i%8; return out;}();
void process_chunk(uint8_t* data,size_t size,uint8_t* entropy) {
    for (int c=0;c<OUT_ITER;++c) {
        size_t i=0;
        for (;i+simd_u8_size<=size;i+=simd_u8_size) {
            batch_u8 b_data,b_entropy;
            b_data=batch_u8::load_unaligned(data+i);
            b_entropy=batch_u8::load_unaligned(entropy+i);
            b_data^=b_entropy;
            for (int y=0;y<IN_ITER;++y) {
                b_data=(b_data<<modulo8_iter[y])|(b_data>>(8-(modulo8_iter[y])));
                b_data^=small_entropy_batches[y];
            }
            b_data.store_unaligned(data+i);
        }
        for (;i<size;++i) {
            uint8_t val=data[i]^entropy[i];
            for (int iter=0;iter<IN_ITER;++iter) {
                val=rotl_table[val][modulo8_iter[iter]];
                val^=small_entropy_buffer[iter];
            }
            data[i]=val;
        }
    }
}
mutex sqe_mutex; 
void process_chunk_write(int fd,uint8_t* data,size_t size,uint8_t* entropy,off_t base_offset,off_t offset_in_block,io_uring* ring) {
    for (int c=0;c<OUT_ITER;++c) {
        size_t i=0;
        for (;i+simd_u8_size<=size;i+=simd_u8_size) {
            batch_u8 b_data,b_entropy;
            b_data=batch_u8::load_unaligned(data+i);
            b_entropy=batch_u8::load_unaligned(entropy+i);
            b_data^=b_entropy;
            for (int y=0;y<IN_ITER;++y) {
                b_data=(b_data<<modulo8_iter[y])|(b_data>>(8-(modulo8_iter[y])));
                b_data^=small_entropy_batches[y];
            }
            b_data.store_unaligned(data+i);
        }
        for (;i<size;++i) {
            uint8_t val=data[i]^entropy[i];
            for (int iter=0;iter<IN_ITER;++iter) {
                val=rotl_table[val][modulo8_iter[iter]];
                val^=small_entropy_buffer[iter];
            }
            data[i]=val;
        }
    }
    std::lock_guard<std::mutex> lock(sqe_mutex);
    io_uring_sqe* sqe=io_uring_get_sqe(ring);
    if (!sqe) throw runtime_error("io_uring_sqe allocation failed");
    io_uring_prep_write(sqe,fd,data,size,base_offset+offset_in_block);
    io_uring_sqe_set_flags(sqe,0);
}
int main(int argc,char* argv[]) {
    if (argc != 2) {
        cout<<"Moulinex v1.0"<<endl;
        cout<<"Usage: moulinex <file>"<<endl;
        return -1;
    }
    const string file_path=argv[1];
    if (!filesystem::exists(file_path) || !filesystem::is_regular_file(file_path)) {
        if (file_path!="-h") {
            cout<<"The provided element isn't suitable for processing."<<endl;
            return -1;
        } else {
            cout<<"Moulinex v1.0"<<endl;
            cout<<"Usage: moulinex <file>"<<endl;
            return 0;
        }
    }
    struct stat st;
    stat(file_path.c_str(),&st);
    const streamsize file_size=st.st_size;
    if (file_size<1) {
        cout<<"Empty file."<<endl;
        return 0;
    }
    struct sysinfo memInfo;
    sysinfo(&memInfo);
    long long free_ram=memInfo.freeram*memInfo.mem_unit;
    const streamsize block_size=512*1024*1024;
    fill_random(small_entropy_buffer.data(),IN_ITER);
    small_entropy_batches.clear();
    for (int y=0;y<IN_ITER;++y) {
        small_entropy_batches.push_back(batch_u8(small_entropy_buffer[y]));
    }
    if (file_size>free_ram/2) {
        cout<<"File is too large for full RAM processing. Shredding by blocks."<<endl;
        int fd=open(file_path.c_str(),O_RDWR);
        if (fd<0) {
            cout<<"Failed to open file for read/write. errno: "<<errno<<endl;
            return -1;
        }
        vector<uint8_t> buffer(block_size);
        vector<chrono::nanoseconds> times;
        ssize_t bytes_read;
        off_t current_pos=0;
        io_uring ring;
        io_uring_queue_init(thread::hardware_concurrency()*2,&ring,0);
        chrono::_V2::system_clock::time_point start=chrono::high_resolution_clock::now();
        int a=0;
        try {
            while ((bytes_read=read(fd,buffer.data(),block_size))>0) {
                unsigned int num_threads=thread::hardware_concurrency();
                if (num_threads>0 && a>0) {
                    for (int i=0;i<num_threads;i++) {
                        io_uring_cqe* cqe;
                        io_uring_wait_cqe(&ring,&cqe);
                        if (cqe->res<0) throw runtime_error("write failed");
                        io_uring_cqe_seen(&ring,cqe);
                    }
                }
                entropy_buffer.resize(bytes_read);
                fill_random(entropy_buffer.data(),bytes_read);
                chrono::_V2::system_clock::time_point start_b=chrono::high_resolution_clock::now();
                if (num_threads==0) {
                    process_chunk(buffer.data(),bytes_read,entropy_buffer.data());
                    if (write(fd,buffer.data(),bytes_read)!=bytes_read) {
                        throw runtime_error("Block writing failed.");
                    }
                } else {
                    vector<thread> threads;
                    vector<size_t> offsets(num_threads+1);
                    size_t chunk_size=bytes_read/num_threads;
                    for (unsigned int i=0;i<num_threads;++i) {
                        offsets[i]=i*chunk_size;
                    }
                    offsets[num_threads]=bytes_read;
                    for (unsigned int i=0;i<num_threads;++i) {
                        size_t current_chunk_size=offsets[i+1]-offsets[i];
                        threads.emplace_back(process_chunk_write,fd,buffer.data()+offsets[i],current_chunk_size,entropy_buffer.data()+offsets[i],current_pos,offsets[i],&ring);
                    }
                    for (auto& t:threads) t.join();
                    io_uring_submit(&ring);
                }
                chrono::_V2::system_clock::time_point end_b=chrono::high_resolution_clock::now();
                auto duration_b=chrono::duration_cast<chrono::nanoseconds>(end_b-start_b);
                times.push_back(duration_b);
                current_pos+=bytes_read;
                cout<<"Processed block at "<<current_pos<<" bytes,size: "<<bytes_read<<" bytes. Duration: "<<duration_b.count()<<" ns"<<endl;
                a++;
            }
        } catch (const exception& e) {
            cout<<"An error occurred during block processing: "<<e.what()<<endl;
            close(fd);
            return -1;
        }
        unsigned int num_threads=thread::hardware_concurrency();
        if (num_threads>0 && a>0) {
            for (int i=0;i<num_threads;i++) {
                io_uring_cqe* cqe;
                io_uring_wait_cqe(&ring,&cqe);
                if (cqe->res<0) throw runtime_error("write failed");
                io_uring_cqe_seen(&ring,cqe);
            }
        }
        auto end=chrono::high_resolution_clock::now();
        auto duration=chrono::duration_cast<chrono::nanoseconds>(end-start);
        uint64_t average=0;
        for (auto i:times) average+=i.count();
        average=average/times.size();
        cout<<"Average time per 512 MB block: "<<average<<" ns"<<endl;
        cout<<"Total Duration (including all the IO): "<<duration.count()<<" ns"<<endl;
        if (fsync(fd)!=0) {
            cout<<"Fsync failed"<<endl;
        }
        close(fd);
    } else {
        cout<<"File size is suitable for full RAM processing."<<endl;
        int fd=open(file_path.c_str(),O_RDWR);
        if (fd<0) {
            cout<<"Failed to open file for read/write. errno: "<<errno<<endl;
            return -1;
        }
        vector<uint8_t> buffer(file_size);
        auto bytes_read=read(fd,buffer.data(),file_size);
        entropy_buffer.resize(file_size);
        fill_random(entropy_buffer.data(),file_size);
        chrono::_V2::system_clock::time_point start=chrono::high_resolution_clock::now();
        unsigned int num_threads=thread::hardware_concurrency();
        io_uring ring;
        io_uring_queue_init(thread::hardware_concurrency()*2,&ring,0);
        if (num_threads==0) {
            process_chunk(buffer.data(),file_size,entropy_buffer.data());
            if (write(fd,buffer.data(),bytes_read)!=bytes_read) {
                cout<<"Failed to write file."<<endl;
                return -1;
            }
        } else {
            vector<thread> threads;
            vector<size_t> offsets(num_threads+1);
            size_t chunk_size=file_size/num_threads;
            for (unsigned int i=0;i<num_threads;++i) {
                offsets[i]=i*chunk_size;
            }
            offsets[num_threads]=file_size;
            for (unsigned int i=0;i<num_threads;++i) {
                size_t current_chunk_size=offsets[i+1]-offsets[i];
                threads.emplace_back(process_chunk_write,fd,buffer.data()+offsets[i],current_chunk_size,entropy_buffer.data()+offsets[i],0,offsets[i],&ring);
            }
            for (auto& t:threads) t.join();
            io_uring_submit(&ring);
            for (int i=0;i<num_threads;i++) {
                io_uring_cqe* cqe;
                io_uring_wait_cqe(&ring,&cqe);
                if (cqe->res<0) throw runtime_error("write failed");
                io_uring_cqe_seen(&ring,cqe);
            }
        }
        auto end=chrono::high_resolution_clock::now();
        auto duration=chrono::duration_cast<chrono::nanoseconds>(end-start);
        cout<<"Duration: "<<duration.count()<<" ns"<<endl;
        if (fsync(fd)!=0) {
            cout<<"Fsync failed"<<endl;
        }
        close(fd);
    }
    cout<<"Finished."<<endl;
    return 0;
}