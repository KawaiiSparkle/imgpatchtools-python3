#!/usr/bin/env python3  
  
import os  
import sys  
import hashlib  
import struct  
import tempfile  
import shutil  
import zlib  
from typing import List, Optional, Tuple, Union  
import subprocess  
import bsdiff4   # type: ignore
import asyncio  
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor  
import mmap  
from pathlib import Path  
import BlockImageUpdate
  
class FileContents:  
    """Represents file contents with SHA1 hash and metadata"""  
    __slots__ = ('data', 'sha1', 'file_path', 'st_mode', 'st_uid', 'st_gid')  # Python 3.13优化：减少内存占用  
      
    def __init__(self, data: bytes = b'', file_path: str = ''):  
        self.data = data  
        self.sha1 = hashlib.sha1(data, usedforsecurity=False).digest() if data else b''  # Python 3.13优化：非安全用途  
        self.file_path = file_path  
        self.st_mode = 0o644  
        self.st_uid = os.getuid() if hasattr(os, 'getuid') else 0  
        self.st_gid = os.getgid() if hasattr(os, 'getgid') else 0  
          
        if file_path and os.path.exists(file_path):  
            stat = os.stat(file_path)  
            self.st_mode = stat.st_mode  
            self.st_uid = stat.st_uid  
            self.st_gid = stat.st_gid  
  
# Constants for ImgDiff  
CHUNK_NORMAL = 0  
CHUNK_GZIP = 1  
CHUNK_DEFLATE = 2  
CHUNK_RAW = 3  
HAS_BSDIFF4 = 1  
  
# Cache path for cross-platform compatibility with Python 3.13  
if os.name == 'nt':  
    CACHE_TEMP_SOURCE = Path(tempfile.gettempdir()) / "applypatch_cache" / "temp_source"  
else:  
    CACHE_TEMP_SOURCE = Path("/tmp/applypatch_cache/temp_source")  
  
def parse_sha1(sha1_str: str) -> bytes:  
    """Parse SHA1 string to bytes, handling format '<digest>:<anything>'"""  
    sha1_part = sha1_str.split(':', 1)[0]  # Python 3.13优化：限制分割次数  
    if len(sha1_part) != 40:  
        raise ValueError(f"Invalid SHA1 length: {len(sha1_part)}")  
      
    try:  
        return bytes.fromhex(sha1_part)  
    except ValueError:  
        raise ValueError(f"Invalid SHA1 format: {sha1_part}")  
  
def short_sha1(sha1_bytes: bytes) -> str:  
    """Convert SHA1 bytes to short hex string"""  
    return sha1_bytes.hex()[:8]  
  
def load_file_contents_mmap(filename: str) -> FileContents:  
    """Load file contents using memory mapping for better performance"""  
    path = Path(filename)  
    if not path.exists():  
        raise FileNotFoundError(f"File not found: {filename}")  
      
    # 对于小文件直接读取，大文件使用mmap  
    file_size = path.stat().st_size  
    if file_size < 1024 * 1024:  # 1MB以下直接读取  
        with open(filename, 'rb') as f:  
            data = f.read()  
    else:  
        # 大文件使用mmap提升性能  
        with open(filename, 'rb') as f:  
            with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:  
                data = bytes(mm)  
      
    return FileContents(data, filename)  
  
def load_file_contents(filename: str) -> FileContents:  
    """Load file contents with optimized path handling"""  
    return load_file_contents_mmap(filename)  
  
def find_matching_patch(file_sha1: bytes, patch_sha1_list: List[str]) -> int:  
    """Find matching patch index for given file SHA1 with early termination"""  
    for i, patch_sha1_str in enumerate(patch_sha1_list):  
        try:  
            patch_sha1 = parse_sha1(patch_sha1_str)  
            if patch_sha1 == file_sha1:  
                return i  
        except ValueError:  
            continue  
    return -1  
  
def read_le_int32(data: bytes, offset: int) -> int:  
    """Read little-endian 32-bit integer"""  
    return struct.unpack_from('<I', data, offset)[0]  # Python 3.13优化：使用unpack_from  
  
def read_le_int64(data: bytes, offset: int) -> int:  
    """Read little-endian 64-bit integer"""  
    return struct.unpack_from('<Q', data, offset)[0]  # Python 3.13优化：使用unpack_from  
  
def apply_bsdiff_patch(source_data: bytes, patch_data: bytes) -> bytes:  
    """Apply BSDiff patch using bsdiff4 module"""  
    if not patch_data.startswith(b'BSDIFF40'):  
        raise ValueError("Not a BSDiff patch")  
      
    if HAS_BSDIFF4 == 1:  
        return bsdiff4.patch(source_data, patch_data)  
    else:  
        raise RuntimeError("bsdiff4 module is required but not available. Install with: pip install bsdiff4")  
  
def apply_imgdiff_patch_optimized(source_data: bytes, patch_data: bytes, bonus_data: bytes = None) -> bytes:  
    """Apply ImgDiff patch with basic error handling"""  
      
    if len(patch_data) < 12:  
        raise ValueError("Patch too short to contain header")  
      
    if not patch_data.startswith(b'IMGDIFF2'):  
        raise ValueError("Not an ImgDiff2 patch")  
      
    num_chunks = read_le_int32(patch_data, 8)  
    pos = 12  
      
    if num_chunks <= 0 or num_chunks > 1000:  
        raise ValueError(f"Invalid number of chunks: {num_chunks}")  
      
    output_chunks = []  
      
    for chunk_idx in range(num_chunks):  
        if pos + 4 > len(patch_data):  
            raise ValueError(f"Failed to read chunk {chunk_idx} record")  
          
        chunk_type = read_le_int32(patch_data, pos)  
        pos += 4  
          
        # 初始化 chunk_data 确保变量存在  
        chunk_data = None  
          
        if chunk_type == CHUNK_NORMAL:  
            chunk_data, pos = process_normal_chunk(chunk_idx, source_data, patch_data, pos)  
        elif chunk_type == CHUNK_RAW:  
            chunk_data, pos = process_raw_chunk(chunk_idx, patch_data, pos)  
        elif chunk_type in (CHUNK_GZIP, CHUNK_DEFLATE):  
            chunk_data, pos = process_compressed_chunk(chunk_idx, chunk_type, source_data, patch_data, pos, bonus_data)  
        else:  
            raise ValueError(f"Unknown chunk type: {chunk_type}")  
          
        if chunk_data is None:  
            raise ValueError(f"Failed to process chunk {chunk_idx}: no data returned")  
          
        output_chunks.append(chunk_data)  
      
    return b''.join(output_chunks)


  
def process_chunk(chunk_idx: int, chunk_type: int, source_data: bytes,     
                 patch_data: bytes, pos: int, bonus_data: bytes = None) -> bytes:  
    """Process individual chunk with type-specific logic and better error handling"""  
      
    # Add validation for chunk type  
    if chunk_type not in (CHUNK_NORMAL, CHUNK_GZIP, CHUNK_DEFLATE, CHUNK_RAW):  
        # Log the problematic chunk type for debugging  
        BlockImageUpdate.logger.error(f"Unknown chunk type: {chunk_type} at position {pos}")  
        BlockImageUpdate.logger.error(f"Patch data around position: {patch_data[max(0, pos-10):pos+10].hex()}")  
        raise ValueError(f"Unknown chunk type: {chunk_type}")  
        
    if chunk_type == CHUNK_NORMAL:  
        if pos + 24 > len(patch_data):  
            raise ValueError(f"Failed to read chunk {chunk_idx} normal header")  
            
        src_start = read_le_int64(patch_data, pos)  
        src_len = read_le_int64(patch_data, pos + 8)  
        patch_offset = read_le_int64(patch_data, pos + 16)  
            
        if src_start + src_len > len(source_data):  
            raise ValueError(f"Source parameters out of range: start={src_start}, len={src_len}")  
            
        chunk_source = source_data[src_start:src_start + src_len]  
        chunk_patch = patch_data[patch_offset:]  
        return apply_bsdiff_patch(chunk_source, chunk_patch)  
            
    elif chunk_type in (CHUNK_GZIP, CHUNK_DEFLATE):  
        if pos + 32 > len(patch_data):  
            raise ValueError(f"Failed to read chunk {chunk_idx} gzip/deflate header")  
            
        src_start = read_le_int64(patch_data, pos)  
        src_len = read_le_int64(patch_data, pos + 8)  
        patch_offset = read_le_int64(patch_data, pos + 16)  
        expanded_len = read_le_int64(patch_data, pos + 24)  
            
        if src_start + src_len > len(source_data):  
            raise ValueError(f"Source parameters out of range: start={src_start}, len={src_len}")  
            
        compressed_source = source_data[src_start:src_start + src_len]  
            
        try:  
            if chunk_type == CHUNK_GZIP:  
                uncompressed_source = zlib.decompress(compressed_source, 16 + zlib.MAX_WBITS)  
            else:  # CHUNK_DEFLATE  
                uncompressed_source = zlib.decompress(compressed_source)  
        except zlib.error as e:  
            raise ValueError(f"Failed to decompress chunk {chunk_idx}: {e}")  
            
        # Handle bonus data  
        if bonus_data and expanded_len > len(uncompressed_source):  
            bonus_size = expanded_len - len(uncompressed_source)  
            if bonus_size <= len(bonus_data):  
                uncompressed_source += bonus_data[:bonus_size]  
            
        chunk_patch = patch_data[patch_offset:]  
        patched_uncompressed = apply_bsdiff_patch(uncompressed_source, chunk_patch)  
            
        try:  
            if chunk_type == CHUNK_GZIP:  
                return zlib.compress(patched_uncompressed, level=6, wbits=16 + zlib.MAX_WBITS)  
            else:  # CHUNK_DEFLATE  
                return zlib.compress(patched_uncompressed, level=6)  
        except zlib.error as e:  
            raise ValueError(f"Failed to recompress chunk {chunk_idx}: {e}")  
                
    elif chunk_type == CHUNK_RAW:  
        if pos + 8 > len(patch_data):  
            raise ValueError(f"Failed to read chunk {chunk_idx} raw header")  
            
        data_len = read_le_int64(patch_data, pos)  
        pos += 8  
            
        if pos + data_len > len(patch_data):  
            raise ValueError(f"Raw chunk data out of range")  
            
        return patch_data[pos:pos + data_len]

def process_normal_chunk(chunk_idx: int, source_data: bytes, patch_data: bytes, pos: int) -> Tuple[bytes, int]:  
    """Process CHUNK_NORMAL"""  
    if pos + 24 > len(patch_data):  
        raise ValueError(f"Failed to read chunk {chunk_idx} normal header")  
      
    src_start = read_le_int64(patch_data, pos)  
    src_len = read_le_int64(patch_data, pos + 8)  
    patch_offset = read_le_int64(patch_data, pos + 16)  
      
    if src_start + src_len > len(source_data):  
        raise ValueError(f"Source parameters out of range: start={src_start}, len={src_len}")  
      
    chunk_source = source_data[src_start:src_start + src_len]  
    chunk_patch = patch_data[patch_offset:]  
    result = apply_bsdiff_patch(chunk_source, chunk_patch)  
    return result, pos + 24  
  
def process_raw_chunk(chunk_idx: int, patch_data: bytes, pos: int) -> Tuple[bytes, int]:      
    """Process CHUNK_RAW with correct length reading and validation"""      
    if pos + 8 > len(patch_data):    # 改为8字节以匹配64位读取  
        raise ValueError(f"Failed to read chunk {chunk_idx} raw header")      
          
    data_len = read_le_int64(patch_data, pos)  # 统一使用64位读取      
    pos += 8      
        
    # 添加数值验证    
    if data_len < 0:    
        raise ValueError(f"Invalid RAW chunk data length: {data_len} (negative)")    
    if data_len > len(patch_data) - pos:    
        raise ValueError(f"Invalid RAW chunk data length: {data_len} (exceeds remaining data)")    
    if data_len > 100 * 1024 * 1024:  # 100MB 限制    
        raise ValueError(f"RAW chunk data too large: {data_len} bytes")    
          
    return patch_data[pos:pos + data_len], pos + data_len




  
def process_compressed_chunk(chunk_idx: int, chunk_type: int, source_data: bytes,     
                           patch_data: bytes, pos: int, bonus_data: bytes = None) -> Tuple[bytes, int]:    
    """Process CHUNK_GZIP or CHUNK_DEFLATE with correct header size calculation"""    
        
    # 基础头部大小都是32字节  
    base_header_size = 32  
        
    if pos + base_header_size > len(patch_data):    
        raise ValueError(f"Failed to read chunk {chunk_idx} compressed header")    
        
    src_start = read_le_int32(patch_data, pos)    
    src_len = read_le_int32(patch_data, pos + 8)    
    patch_offset = read_le_int32(patch_data, pos + 16)    
    expanded_len = read_le_int32(patch_data, pos + 24)    
        
    # 对于 CHUNK_DEFLATE，读取额外的压缩参数  
    compression_level = 6  
    window_bits = -15  
    mem_level = 8  
    strategy = 0  
      
    actual_header_size = base_header_size  
      
    if chunk_type == CHUNK_DEFLATE:  
        # 检查是否有足够的数据读取额外参数  
        if pos + 48 <= len(patch_data):  # 32 + 16 = 48字节  
            level = read_le_int32(patch_data, pos + 32)  
            window_bits_raw = read_le_int32(patch_data, pos + 36)  
            mem_level_raw = read_le_int32(patch_data, pos + 40)  
            strategy_raw = read_le_int32(patch_data, pos + 44)  
              
            # 验证参数范围  
            if 0 <= level <= 9:  
                compression_level = level  
            if -15 <= window_bits_raw <= 15:  
                window_bits = window_bits_raw  
            if 1 <= mem_level_raw <= 9:  
                mem_level = mem_level_raw  
            if 0 <= strategy_raw <= 4:  
                strategy = strategy_raw  
                  
            actual_header_size = 48  
        else:  
            # 如果没有足够数据，使用默认值  
            BlockImageUpdate.logger.warning(f"DEFLATE chunk {chunk_idx} missing compression parameters, using defaults")  
        
    if src_start + src_len > len(source_data):    
        raise ValueError(f"Source parameters out of range: start={src_start}, len={src_len}")    
        
    compressed_source = source_data[src_start:src_start + src_len]    
        
    # 解压缩源数据    
    try:    
        if chunk_type == CHUNK_GZIP:    
            uncompressed_source = zlib.decompress(compressed_source, 16 + zlib.MAX_WBITS)    
        else:  # CHUNK_DEFLATE    
            try:    
                uncompressed_source = zlib.decompress(compressed_source, window_bits)    
            except zlib.error:    
                uncompressed_source = zlib.decompress(compressed_source, -zlib.MAX_WBITS)    
    except zlib.error as e:    
        raise ValueError(f"Failed to decompress chunk {chunk_idx}: {e}")    
        
    # 处理 bonus data - 改进逻辑，不限制chunk_idx  
    if bonus_data and expanded_len > len(uncompressed_source):    
        bonus_size = expanded_len - len(uncompressed_source)    
        if bonus_size <= len(bonus_data):    
            uncompressed_source += bonus_data[:bonus_size]    
            BlockImageUpdate.logger.debug(f"Applied {bonus_size} bytes of bonus data to chunk {chunk_idx}")  
        
    # 应用 bsdiff 补丁    
    chunk_patch = patch_data[patch_offset:]    
    patched_uncompressed = apply_bsdiff_patch(uncompressed_source, chunk_patch)    
        
    # 重新压缩    
    try:    
        if chunk_type == CHUNK_GZIP:    
            result = zlib.compress(patched_uncompressed, level=compression_level, wbits=16 + zlib.MAX_WBITS)    
        else:  # CHUNK_DEFLATE    
            # 使用读取的压缩参数  
            compressor = zlib.compressobj(level=compression_level, wbits=window_bits,   
                                        memLevel=mem_level, strategy=strategy)  
            result = compressor.compress(patched_uncompressed) + compressor.flush()  
    except zlib.error as e:    
        raise ValueError(f"Failed to recompress chunk {chunk_idx}: {e}")    
        
    return result, pos + actual_header_size

  
def update_pos_for_chunk_type(chunk_type: int, patch_data: bytes, pos: int) -> int:    
    """Update position based on chunk type with better validation"""    
    if chunk_type == CHUNK_NORMAL:    
        new_pos = pos + 24    
    elif chunk_type == CHUNK_GZIP:  
        new_pos = pos + 32  
    elif chunk_type == CHUNK_DEFLATE:  
        # 检查是否有扩展头部  
        if pos + 48 <= len(patch_data):  
            new_pos = pos + 48  # 包含压缩参数  
        else:  
            new_pos = pos + 32  # 只有基础头部  
    elif chunk_type == CHUNK_RAW:    
        if pos + 8 > len(patch_data):    
            raise ValueError("Cannot read RAW chunk length")    
        data_len = read_le_int64(patch_data, pos)  # 统一使用64位  
        if data_len < 0 or data_len > len(patch_data):    
            raise ValueError(f"Invalid RAW chunk data length: {data_len}")    
        new_pos = pos + 8 + data_len    
    else:    
        raise ValueError(f"Unknown chunk type: {chunk_type}")    
        
    # Validate new position    
    if new_pos > len(patch_data):    
        raise ValueError(f"Position {new_pos} exceeds patch data length {len(patch_data)}")    
        
    return new_pos

                 
async def generate_target_async(source_file: FileContents, patch_data: bytes,    
                               target_filename: str, target_sha1: bytes,    
                               target_size: int, bonus_data: bytes = None) -> bool:    
    """Async version of target generation with enhanced error handling"""    
        
    # 检查是否为分区目标，转换为文件写入    
    if target_filename.startswith(('MTD:', 'EMMC:')):    
        parts = target_filename.split(':')    
        if len(parts) >= 2:    
            partition_name = parts[1].replace('/', '_').replace('\\', '_')    
            target_filename = f"patched_{partition_name}.img"    
            print(f"Converting partition target to file: {target_filename}")    
        
    retry_count = 2    
    for attempt in range(retry_count):    
        try:    
            # 异步应用补丁    
            if patch_data.startswith(b'BSDIFF40'):    
                # 对于大文件使用进程池    
                if len(source_file.data) > 10 * 1024 * 1024:  # 10MB以上    
                    loop = asyncio.get_event_loop()    
                    try:  
                        with ProcessPoolExecutor() as executor:    
                            patched_data = await loop.run_in_executor(    
                                executor, apply_bsdiff_patch, source_file.data, patch_data    
                            )  
                    except (OSError, MemoryError) as e:  
                        print(f"Process pool execution failed: {e}")  
                        if attempt < retry_count - 1:  
                            print("Retrying with synchronous processing...")  
                            patched_data = apply_bsdiff_patch(source_file.data, patch_data)  
                        else:  
                            return False  
                    except Exception as e:  
                        print(f"Unexpected error in process pool: {e}")  
                        if attempt < retry_count - 1:  
                            print("Retrying...")  
                            continue  
                        return False  
                else:    
                    patched_data = apply_bsdiff_patch(source_file.data, patch_data)    
            elif patch_data.startswith(b'IMGDIFF2'):    
                try:  
                    patched_data = apply_imgdiff_patch_optimized(source_file.data, patch_data, bonus_data)  
                except (ValueError, zlib.error) as e:  
                    print(f"ImgDiff patch failed: {e}")  
                    if attempt < retry_count - 1:  
                        print("Retrying...")  
                        continue  
                    return False  
            else:    
                print("Unknown patch format")    
                return False    
                
            # 验证大小    
            if len(patched_data) != target_size:    
                print(f"Size mismatch: expected {target_size}, got {len(patched_data)}")    
                if attempt < retry_count - 1:    
                    print("Retrying...")    
                    continue    
                return False    
                
            # Python 3.13优化：并行计算SHA1    
            loop = asyncio.get_event_loop()    
            try:  
                actual_sha1 = await loop.run_in_executor(    
                    None, lambda: hashlib.sha1(patched_data, usedforsecurity=False).digest()    
                )  
            except Exception as e:  
                print(f"SHA1 calculation failed: {e}")  
                if attempt < retry_count - 1:  
                    print("Retrying...")  
                    continue  
                return False  
                
            if actual_sha1 != target_sha1:    
                print(f"SHA1 mismatch: expected {target_sha1.hex()}, got {actual_sha1.hex()}")    
                if attempt < retry_count - 1:    
                    print("Retrying...")    
                    continue    
                return False    
                
            # 原子写入文件    
            temp_filename = target_filename + '.patch'    
            try:    
                # Python 3.13优化：使用异步文件写入    
                loop = asyncio.get_event_loop()    
                await loop.run_in_executor(None, write_file_atomic, temp_filename, patched_data, source_file)    
                return True    
                    
            except (OSError, PermissionError) as e:    
                print(f"Failed to write target file: {e}")    
                if Path(temp_filename).exists():    
                    try:    
                        Path(temp_filename).unlink()    
                    except Exception:    
                        pass    
                if attempt < retry_count - 1:    
                    print("Retrying...")    
                    continue    
                return False    
                    
        except (MemoryError, OSError) as e:    
            print(f"System error during patch application: {e}")    
            if attempt < retry_count - 1:    
                print("Retrying...")    
                continue    
            return False  
        except Exception as e:    
            print(f"Unexpected error during patch application: {e}")    
            if attempt < retry_count - 1:    
                print("Retrying...")    
                continue    
            return False    
        
    return False
  
def write_file_atomic(temp_filename: str, data: bytes, source_file: FileContents):  
    """Atomic file write with proper permissions"""  
    with open(temp_filename, 'wb') as f:  
        f.write(data)  
      
    # 设置文件权限（跨平台兼容）  
    try:  
        os.chmod(temp_filename, source_file.st_mode)  
        if hasattr(os, 'chown') and os.name != 'nt':  # 非Windows系统  
            os.chown(temp_filename, source_file.st_uid, source_file.st_gid)  
    except (OSError, PermissionError):  
        pass  # 忽略权限错误  
      
    # 原子重命名  
    target_filename = temp_filename.replace('.patch', '')  
    if os.name == 'nt':  # Windows需要先删除目标文件  
        if os.path.exists(target_filename):  
            os.unlink(target_filename)  
    os.rename(temp_filename, target_filename)  
  
def generate_target_with_retry(source_file: FileContents, patch_data: bytes,  
                              target_filename: str, target_sha1: bytes,  
                              target_size: int, bonus_data: bytes = None) -> bool:  
    """Synchronous wrapper for async target generation"""  
    try:  
        # Python 3.13优化：使用asyncio运行异步函数  
        return asyncio.run(generate_target_async(source_file, patch_data, target_filename,  
                                               target_sha1, target_size, bonus_data))  
    except Exception as e:  
        print(f"Async execution failed, falling back to sync: {e}")  
        # 回退到同步版本  
        return generate_target_sync(source_file, patch_data, target_filename,  
                                  target_sha1, target_size, bonus_data)  
  
def generate_target_sync(source_file: FileContents, patch_data: bytes,  
                        target_filename: str, target_sha1: bytes,  
                        target_size: int, bonus_data: bytes = None) -> bool:  
    """Synchronous fallback version"""  
    # 检查是否为分区目标，转换为文件写入  
    if target_filename.startswith(('MTD:', 'EMMC:')):  
        parts = target_filename.split(':')  
        if len(parts) >= 2:  
            partition_name = parts[1].replace('/', '_').replace('\\', '_')  
            target_filename = f"patched_{partition_name}.img"  
            print(f"Converting partition target to file: {target_filename}")  
      
    retry_count = 2  
    for attempt in range(retry_count):  
        try:  
            # 应用补丁  
            if patch_data.startswith(b'BSDIFF40'):  
                patched_data = apply_bsdiff_patch(source_file.data, patch_data)  
            elif patch_data.startswith(b'IMGDIFF2'):  
                patched_data = apply_imgdiff_patch_optimized(source_file.data, patch_data, bonus_data)  
            else:  
                print("Unknown patch format")  
                return False  
              
            # 验证大小  
            if len(patched_data) != target_size:  
                print(f"Size mismatch: expected {target_size}, got {len(patched_data)}")  
                if attempt < retry_count - 1:  
                    print("Retrying...")  
                    continue  
                return False  
              
            # 验证SHA1  
            actual_sha1 = hashlib.sha1(patched_data, usedforsecurity=False).digest()  
            if actual_sha1 != target_sha1:  
                print(f"SHA1 mismatch: expected {target_sha1.hex()}, got {actual_sha1.hex()}")  
                if attempt < retry_count - 1:  
                    print("Retrying...")  
                    continue  
                return False  
              
            # 原子写入文件  
            temp_filename = target_filename + '.patch'  
            try:  
                write_file_atomic(temp_filename, patched_data, source_file)  
                return True  
                  
            except Exception as e:  
                print(f"Failed to write target file: {e}")  
                if Path(temp_filename).exists():  
                    try:  
                        Path(temp_filename).unlink()  
                    except Exception:  
                        pass  
                if attempt < retry_count - 1:  
                    print("Retrying...")  
                    continue  
                return False  
                  
        except Exception as e:  
            print(f"Patch application failed: {e}")  
            if attempt < retry_count - 1:  
                print("Retrying...")  
                continue  
            return False  
      
    return False  
  
def save_file_contents(filename: str, file_contents: FileContents) -> bool:  
    """Save file contents to disk with proper permissions"""  
    try:  
        # 确保目录存在  
        Path(filename).parent.mkdir(parents=True, exist_ok=True)  
          
        with open(filename, 'wb') as f:  
            f.write(file_contents.data)  
          
        # 设置文件权限（跨平台兼容）  
        try:  
            os.chmod(filename, file_contents.st_mode)  
            if hasattr(os, 'chown') and os.name != 'nt':  # 非Windows系统  
                os.chown(filename, file_contents.st_uid, file_contents.st_gid)  
        except (OSError, PermissionError):  
            pass  # 忽略权限错误  
          
        return True  
    except Exception as e:  
        print(f"Failed to save file {filename}: {e}")  
        return False  
  
def applypatch_check(filename: str, patch_sha1_list: List[str]) -> bool:  
    """Check if file matches any of the expected SHA1 sums"""  
    try:  
        file_contents = load_file_contents(filename)  
        if not patch_sha1_list:  
            return True  # No SHA1s to check  
          
        return find_matching_patch(file_contents.sha1, patch_sha1_list) >= 0  
    except Exception:  
        return False  
  
def applypatch_with_cache(source_filename: str, target_filename: str,  
                         target_sha1_str: str, target_size: int,  
                         patch_sha1_list: List[str], patch_files: List[str],  
                         bonus_data: bytes = None) -> int:  
    """Enhanced applypatch with cache and recovery support"""  
      
    print(f"patch {source_filename}:")  
      
    if target_filename == "-":  
        target_filename = source_filename  
      
    try:  
        target_sha1 = parse_sha1(target_sha1_str)  
    except ValueError as e:  
        print(f"Failed to parse target SHA1: {e}")  
        return 1  
      
    # 检查目标文件是否已经是正确的  
    try:  
        if os.path.exists(target_filename):  
            target_contents = load_file_contents(target_filename)  
            if target_contents.sha1 == target_sha1:  
                print(f"already {short_sha1(target_sha1)}")  
                return 0  
    except Exception:  
        pass  
      
    # 尝试加载源文件  
    source_file = None  
    try:  
        source_file = load_file_contents(source_filename)  
    except Exception as e:  
        print(f"Failed to load source file: {e}")  
      
    # 查找匹配的补丁  
    patch_index = -1  
    if source_file:  
        patch_index = find_matching_patch(source_file.sha1, patch_sha1_list)  
      
    # 如果源文件不匹配，尝试从缓存加载  
    if patch_index < 0:  
        print("source file is bad; trying copy")  
        try:  
            if CACHE_TEMP_SOURCE.exists():  
                copy_file = load_file_contents(str(CACHE_TEMP_SOURCE))  
                patch_index = find_matching_patch(copy_file.sha1, patch_sha1_list)  
                if patch_index >= 0:  
                    source_file = copy_file  
                    print("using cached source file")  
        except Exception:  
            pass  
      
    if patch_index < 0:  
        print("copy file doesn't match source SHA-1s either")  
        return 1  
      
    # 创建源文件备份到缓存  
    try:  
        CACHE_TEMP_SOURCE.parent.mkdir(parents=True, exist_ok=True)  
        save_file_contents(str(CACHE_TEMP_SOURCE), source_file)  
    except Exception as e:  
        print(f"Warning: failed to back up source file: {e}")  
      
    # 加载补丁数据  
    try:  
        with open(patch_files[patch_index], 'rb') as f:  
            patch_data = f.read()  
    except Exception as e:  
        print(f"Failed to load patch file: {e}")  
        return 1  
      
    # 应用补丁  
    if generate_target_with_retry(source_file, patch_data, target_filename,  
                                 target_sha1, target_size, bonus_data):  
        print(f"now {short_sha1(target_sha1)}")  
        # 清理缓存  
        try:  
            if CACHE_TEMP_SOURCE.exists():  
                CACHE_TEMP_SOURCE.unlink()  
        except Exception:  
            pass  
        return 0  
    else:  
        return 1  
  
def check_python_version():  
    """Check Python version compatibility"""  
    if sys.version_info < (3, 8):  
        print("Warning: This script is designed for Python 3.8+")  
        print(f"Current version: {sys.version}")  
    elif sys.version_info >= (3, 13):  
        print(f"Running on Python {sys.version_info.major}.{sys.version_info.minor}")  
def main():  
    """Main function with enhanced error handling for Python 3.13"""  
    if len(sys.argv) < 7:  
        print(f"usage: {sys.argv[0]} <file> <target> <tgt_sha1> <size> <init_sha1(1)> <patch(1)> [init_sha1(2)] [patch(2)] ... [bonus]")  
        print("\t<file> = source file from rom zip")  
        print("\t<target> = target file (use \"-\" to patch source file)")  
        print("\t<tgt_sha1> = target SHA1 Sum after patching")  
        print("\t<size> = file size")  
        print("\t<init_sha1> = file SHA1 sum")  
        print("\t<patch> = patch file (.p) from OTA zip")  
        print("\t<bonus> = bonus resource file")  
        return 1  
      
    source_filename = sys.argv[1]  
    target_filename = sys.argv[2]  
    target_sha1_str = sys.argv[3]  
    target_size_str = sys.argv[4]  
      
    try:  
        target_size = int(target_size_str)  
    except ValueError:  
        print(f"Invalid size: {target_size_str}")  
        return 1  
      
    # Parse patch arguments  
    remaining_args = sys.argv[5:]  
    bonus_data = None  
      
    # Check if there's a bonus file (odd number of remaining args)  
    if len(remaining_args) % 2 != 0:  
        bonus_filename = remaining_args[-1]  
        patch_args = remaining_args[:-1]  
          
        # Load bonus data  
        try:  
            with open(bonus_filename, 'rb') as f:  
                bonus_data = f.read()  
        except Exception as e:  
            print(f"Failed to load bonus file: {e}")  
            return 1  
    else:  
        patch_args = remaining_args  
      
    if len(patch_args) % 2 != 0:  
        print("Invalid number of patch arguments")  
        return 1  
      
    # Extract SHA1s and patch files  
    patch_sha1_list = []  
    patch_files = []  
    for i in range(0, len(patch_args), 2):  
        patch_sha1_list.append(patch_args[i])  
        patch_files.append(patch_args[i + 1])  
      
    # Create cache directory with Python 3.13 compatibility  
    if os.name == 'nt':  
        cache_dir = Path(tempfile.gettempdir()) / "applypatch_cache"  
    else:  
        cache_dir = Path("/tmp/applypatch_cache")  
      
    try:  
        cache_dir.mkdir(parents=True, exist_ok=True)  
        print(f"cache dir: {cache_dir}")  
    except Exception as e:  
        print(f"Failed to create cache directory: {e}")  
        return 1  
      
    # Apply the patch using the enhanced function  
    result = applypatch_with_cache(source_filename, target_filename, target_sha1_str,  
                                  target_size, patch_sha1_list, patch_files, bonus_data)  
      
    # Clean up cache directory  
    try:  
        if cache_dir.exists():  
            shutil.rmtree(cache_dir)  
    except Exception as e:  
        print(f"Failed to remove cache directory: {e}")  
      
    if result == 0:  
        print("Done")  
    else:  
        print(f"Done with error code: {result}")  
      
    return result  
  
if __name__ == "__main__":  
    check_python_version()  
    sys.exit(main())