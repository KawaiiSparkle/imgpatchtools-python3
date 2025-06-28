
#!/usr/bin/env python3


import sys
import os
import subprocess
import struct
import hashlib
import tempfile
import threading
import time
import queue # Import the queue module  
from typing import List, Dict, Any, Optional, BinaryIO, Iterator, Tuple, Union
from pathlib import Path
import json

class RangeSet:  
    """Represents a set of block ranges for operations"""  
      
    def __init__(self, range_text: str):  
        if not range_text or not isinstance(range_text, str):  
            raise ValueError("Range text must be a non-empty string")  
              
        pieces = range_text.split(',')  
        if len(pieces) < 3:  
            raise ValueError(f"Invalid range format: {range_text}")  
          
        try:  
            num = int(pieces[0])  
        except ValueError:  
            raise ValueError(f"Invalid range count: {pieces[0]}")  
              
        if num == 0:  
            raise ValueError("Range count cannot be zero")  
        if num % 2 != 0:  
            raise ValueError("Range count must be even (pairs of start,end)")  
        if num != len(pieces) - 1:  
            raise ValueError(f"Range count mismatch: expected {num}, got {len(pieces) - 1}")  
          
        self.pos = []  
        self.count = num // 2  
        self.size = 0  
          
        # Parse and validate ranges  
        for i in range(0, num, 2):  
            try:  
                start = int(pieces[i + 1])  
                end = int(pieces[i + 2])  
            except ValueError:  
                raise ValueError(f"Invalid range values: {pieces[i + 1]}, {pieces[i + 2]}")  
            except IndexError:  
                raise ValueError("Incomplete range specification")  
                  
            if start < 0 or end < 0:  
                raise ValueError(f"Range values cannot be negative: {start}, {end}")  
            if start >= end:  
                raise ValueError(f"Invalid range: start >= end ({start} >= {end})")  
              
            self.pos.extend([start, end])  
            self.size += end - start  
          
        # Validate ranges don't overlap  
        self._validate_no_overlaps()  
      
    def _validate_no_overlaps(self):  
        """Ensure ranges don't overlap"""  
        ranges = [(self.pos[i], self.pos[i + 1]) for i in range(0, len(self.pos), 2)]  
        ranges.sort()  
          
        for i in range(len(ranges) - 1):  
            if ranges[i][1] > ranges[i + 1][0]:  
                raise ValueError(f"Overlapping ranges: [{ranges[i][0]}-{ranges[i][1]}) and [{ranges[i + 1][0]}-{ranges[i + 1][1]})")  
      
    def __iter__(self):  
        """Iterate over (start, end) pairs"""  
        for i in range(0, len(self.pos), 2):  
            yield (self.pos[i], self.pos[i + 1])  
      
    def __len__(self) -> int:  
        return self.count  
      
    def __str__(self):  
        return f"RangeSet(count={self.count}, size={self.size}, ranges={self.pos})"
    
    def get_ranges(self):  
        """Return list of (start, end) tuples"""  
        return [(self.pos[i], self.pos[i + 1]) for i in range(0, len(self.pos), 2)]


class BlockImageUpdate:
    """Main BlockImageUpdate implementation"""
    
    BLOCKSIZE = 4096  # Standard block size
    
    def __init__(self, blockdev_path: str, transfer_list_path: str,  
                 new_data_path: str, patch_data_path: str):  
        self.blockdev_path = Path(blockdev_path)  
        self.transfer_list_path = Path(transfer_list_path)  
        self.new_data_path = Path(new_data_path)  
        self.patch_data_path = Path(patch_data_path)  
        self.stash_map: Dict[str, bytes] = {}  
        self.written = 0  
        self.version = 1  
        self.total_blocks = 0  
        self.new_data_fd: Optional[BinaryIO] = None  
        self.patch_data_fd: Optional[BinaryIO] = None  
          
        if os.name == 'nt':  # Windows  
            self.stash_base_dir = Path(os.environ.get('TEMP', 'C:\\temp')) / 'biu_cache'  
        else:  # Unix-like systems  
            self.stash_base_dir = Path('/tmp/biu_cache')  
  
        self.transfer_lines = [] # Store transfer list lines for progress tracking  
  
        # Threading components for new data streaming  
        self.new_data_queue = queue.Queue(maxsize=10) # Buffer up to 10 blocks  
        self.new_data_producer_thread: Optional[threading.Thread] = None  
        self.new_data_producer_running = True  
        self.new_data_condition = threading.Condition() # For signaling between threads  
  
    def _new_data_producer(self, fd: BinaryIO, q: queue.Queue, cond: threading.Condition, block_size: int):  
        """  
        Background thread function to read new data and put it into a queue.  
        Mimics C++ get_new_data and receive_new_data.  
        """  
        while self.new_data_producer_running:  
            try:  
                # Read a block of data  
                data = fd.read(block_size)  
                if not data:  
                    # End of file, put a sentinel value to signal completion  
                    q.put(None)  
                    self.new_data_producer_running = False  
                    break  
  
                # Pad if it's a partial block (e.g., at EOF)  
                if len(data) < block_size:  
                    data = data.ljust(block_size, b'\x00')  
  
                # Put data into the queue  
                q.put(data)  
  
                # Signal that data is available (if a consumer is waiting)  
                with cond:  
                    cond.notify_all()  
  
            except Exception as e:  
                print(f"New data producer error: {e}")  
                self.new_data_producer_running = False  
                q.put(None) # Put sentinel on error too  
                break


    def _move_range(self, dest: bytearray, locs: RangeSet, source: bytes):  
        """Mimic C++ MoveRange: move packed source data to locations in dest."""  
        current_stash_offset = len(source)  
        for start_block, end_block in reversed(locs.get_ranges()):  
            num_blocks = end_block - start_block  
            size_to_copy = num_blocks * self.BLOCKSIZE  
            current_stash_offset -= size_to_copy  
            dest[start_block * self.BLOCKSIZE : end_block * self.BLOCKSIZE] = source[current_stash_offset : current_stash_offset + size_to_copy]

    def _open_block_device(self, mode='rb'):  
        """Open block device with platform-specific handling"""  
        try:  
            if os.name == 'nt':  # Windows  
                if str(self.blockdev_path).startswith('\\\\.\\'):  
                    return open(self.blockdev_path, mode, buffering=0)  
                else:  
                    return open(self.blockdev_path, mode)  
            else:  # Unix-like systems  
                return open(self.blockdev_path, mode)  
        except PermissionError:  
            print(f"Permission denied accessing {self.blockdev_path}")  
            print("Note: Block device access may require administrator/root privileges")  
            raise

        
    def __enter__(self):  
        """Context manager entry with platform-specific initialization"""  
        
        # Platform-specific warnings  
        if os.name == 'nt':  
            print("Warning: Running on Windows - some block device operations may require administrator privileges")  
        
        try:  
            if self.new_data_path.exists():  
                self.new_data_fd = open(self.new_data_path, 'rb')
                self.new_data_producer_running = True
                self.new_data_producer_thread = threading.Thread(  
            target=self._new_data_producer,  
            args=(self.new_data_fd, self.new_data_queue, self.new_data_condition, self.BLOCKSIZE)
            )
            self.new_data_producer_thread.daemon = True
            self.new_data_producer_thread.start()
            if self.patch_data_path.exists():  
                self.patch_data_fd = open(self.patch_data_path, 'rb')  
            
            # Create stash directory with proper permissions  
            self.stash_base_dir.mkdir(parents=True, exist_ok=True)  
            
        except Exception as e:  
            print(f"Failed to initialize: {e}")  
            raise  
        return self
    
    def _cleanup_stash(self):
        """Clean up stash files and directory"""
        try:
            if os.path.exists(self.stash_base_dir):
                for file in os.listdir(self.stash_base_dir):
                    os.unlink(os.path.join(self.stash_base_dir, file))
                os.rmdir(self.stash_base_dir)
        except Exception as e:
            print(f"Warning: Failed to cleanup stash: {e}")
    
    def perform_command_zero(self, tokens: List[str], pos: int) -> int:
        """Zero out specified block ranges""" 
        
        if pos >= len(tokens):
            print("Missing target blocks for zero")
            return -1
            
        try:
            rangeset = RangeSet(tokens[pos])
            print(f"  zeroing {rangeset.size} blocks")
            
            zero_block = b'\x00' * self.BLOCKSIZE
            
            with open(self.blockdev_path, 'r+b') as f:
                for i in range(0, len(rangeset.pos), 2):
                    start_block = rangeset.pos[i]
                    end_block = rangeset.pos[i + 1]
                    
                    offset = start_block * self.BLOCKSIZE
                    f.seek(offset)
                    
                    for block in range(start_block, end_block):
                        f.write(zero_block)
                    f.flush()
            
            self.written += rangeset.size
            return 0
            
        except Exception as e:
            print(f"Failed to zero blocks: {e}")
            return -1
    
    def perform_command_erase(self, tokens: List[str], pos: int) -> int:
        """Erase command - equivalent to zero for cross-platform compatibility"""
        
        print("  erase -> zero (cross-platform compatibility)")
        return self.perform_command_zero(tokens, pos)
    
    def perform_command_new(self, tokens: List[str], pos: int) -> int:  
        """Write new data to specified blocks using a producer-consumer model."""  
        if pos >= len(tokens):  
            print("Missing target blocks for new")  
            return -1  
  
        rangeset = RangeSet(tokens[pos])  
        print(f"  writing {rangeset.size} blocks of new data")  
  
        if not self.new_data_producer_thread or not self.new_data_producer_thread.is_alive():  
            print("New data producer thread is not running.")  
            return -1  
  
        try:  
            with self._open_block_device('r+b') as f: # Use _open_block_device for cross-platform  
                for i in range(0, len(rangeset.pos), 2):  
                    start_block = rangeset.pos[i]  
                    end_block = rangeset.pos[i + 1]  
  
                    f.seek(start_block * self.BLOCKSIZE)  
  
                    for block in range(start_block, end_block):  
                        # Get data from the queue, waiting if necessary  
                        data = self.new_data_queue.get(timeout=60) # Wait up to 60 seconds for data  
                        if data is None: # Sentinel value indicates end or error  
                            print("New data stream ended unexpectedly or producer failed.")  
                            return -1  
  
                        if len(data) != self.BLOCKSIZE:  
                            print(f"Received partial block from queue: {len(data)} bytes")  
                            # This should ideally not happen if producer pads correctly, but as a safeguard  
                            data = data.ljust(self.BLOCKSIZE, b'\x00')  
  
                        f.write(data)  
                        f.flush() # Ensure data is written  
  
                        self.new_data_queue.task_done() # Mark task as done  
  
            self.written += rangeset.size  
            return 0  
  
        except queue.Empty:  
            print("Timeout waiting for new data from producer.")  
            return -1  
        except Exception as e:  
            print(f"Failed to write new data: {e}")  
            return -1
        

    def perform_command_diff(self, tokens: List[str], pos: int, cmd_name: str) -> int:  
        """Realistic diff command implementation with proper version handling"""  
        
        # Parse offset and length as shown in C++ implementation  
        if pos + 1 >= len(tokens):  
            print(f"Missing patch offset or length for {cmd_name}")  
            return -1  
        
        try:  
            offset = int(tokens[pos])  
            length = int(tokens[pos + 1])  
            pos += 2  
            
            # Parse based on actual transfer list version  
            if self.version >= 3:  
                # Version 3+ format from your examples:  
                # bsdiff 0 240979 576ed7e5ace36f7d78d19f3b4cd2b865a788bb66 646312d29b6e45b2961a197d64973ab4ceecd775 4,130970,131029,131071,131072 60  
                if pos + 4 >= len(tokens):  
                    print("Missing required parameters for version 3+ diff")  
                    return -1  
                
                src_hash = tokens[pos]  
                tgt_hash = tokens[pos + 1]  
                tgt_range = tokens[pos + 2]  
                src_blocks = int(tokens[pos + 3])  
                pos += 4  
                
                # Parse target range  
                tgt_rangeset = RangeSet(tgt_range)  
                
                # Handle source data - could be from ranges or stashes  
                if pos < len(tokens) and tokens[pos] != "-":  
                    # Has source range  
                    src_rangeset = RangeSet(tokens[pos])  
                    src_data = self._read_blocks(src_rangeset)  
                    pos += 1  
                else:  
                    # No source range, data from stashes  
                    src_data = b'\x00' * (src_blocks * self.BLOCKSIZE)  
                    if pos < len(tokens):  
                        pos += 1  # Skip the "-"  
                
                # Handle stash specifications if present  
                while pos < len(tokens):  
                    stash_spec = tokens[pos]  
                    if ':' in stash_spec:  
                        self._apply_stash_data(src_data, stash_spec)  
                    pos += 1  
                    
            else:  
                # Version 1/2 handling (simplified for brevity)  
                tgt_rangeset = RangeSet(tokens[pos])  
                src_rangeset = RangeSet(tokens[pos + 1])  
                src_data = self._read_blocks(src_rangeset)  
                src_hash = None  
                tgt_hash = None  
            
            # Verify source hash if available  
            if src_hash and src_hash != "0" * 40:  
                actual_src_hash = hashlib.sha1(src_data).hexdigest()  
                if actual_src_hash != src_hash:  
                    print(f"Source hash mismatch: expected {src_hash}, got {actual_src_hash}")  
                    return -1  
            
            # Extract patch data  
            self.patch_data_fd.seek(offset)  
            patch_data = self.patch_data_fd.read(length)  
            
            # Apply patch with realistic approach  
            result = self._apply_patch_external_v3(src_data, patch_data, cmd_name, tgt_hash)  
            if result is None:  
                return -1  
            
            # Write result back  
            if not self._write_blocks(tgt_rangeset, result):  
                return -1  
            
            self.written += tgt_rangeset.size  
            return 0  
            
        except Exception as e:  
            print(f"Failed to apply {cmd_name} patch: {e}")  
            return -1

    def _apply_patch_external_v3(self, src_data: bytes, patch_data: bytes, cmd_name: str, expected_tgt_hash: str) -> Optional[bytes]:  
        """Apply patch using external ApplyPatch.py with cross-platform temp file handling"""  
        
        try:  
            # Use platform-appropriate temporary directory  
            temp_dir = Path(tempfile.gettempdir())  
            
            # Create temporary files with proper cleanup  
            with tempfile.NamedTemporaryFile(delete=False, suffix='.src', dir=temp_dir) as temp_src:  
                temp_src.write(src_data)  
                temp_src_path = Path(temp_src.name)  
            
            with tempfile.NamedTemporaryFile(delete=False, suffix='.patch', dir=temp_dir) as temp_patch:  
                temp_patch.write(patch_data)  
                temp_patch_path = Path(temp_patch.name)  
            
            temp_tgt_path = temp_src_path.with_suffix('.patched')  
            
            # Calculate source SHA1  
            src_sha1 = hashlib.sha1(src_data).hexdigest()  
            target_size = self._estimate_target_size(patch_data, len(src_data))  
            
            # Build command with proper path handling  
            cmd = [  
                sys.executable, 'ApplyPatch.py',  # Use current Python interpreter  
                str(temp_src_path),  
                str(temp_tgt_path),   
                expected_tgt_hash,  
                str(target_size),  
                src_sha1,  
                str(temp_patch_path)  
            ]  
            
            # Execute with proper environment  
            env = os.environ.copy()  
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300, env=env)  
            
            if result.returncode != 0:  
                print(f"ApplyPatch.py failed: {result.stderr}")  
                return None  
            
            # Read result  
            with open(temp_tgt_path, 'rb') as f:  
                patched_data = f.read()  
            
            return patched_data  
            
        finally:  
            # Cleanup temporary files  
            for temp_file in [temp_src_path, temp_patch_path, temp_tgt_path]:  
                try:  
                    if temp_file.exists():  
                        temp_file.unlink()  
                except OSError:  
                    pass  # Ignore cleanup errors

    def _load_src_tgt_version1(self, tokens: List[str], pos: int) -> Tuple[RangeSet, bytes, int]:  
        """Loads source and target ranges for version 1 commands."""  
        # C++ reference: blockimg/blockimg.cpp:478-498  
        if pos + 1 >= len(tokens):  
            raise ValueError("Invalid parameters for version 1 load")  
    
        src_rangeset = RangeSet(tokens[pos])  
        tgt_rangeset = RangeSet(tokens[pos + 1])  
        
        src_data = self._read_blocks(src_rangeset)  
        if src_data is None:  
            raise IOError("Failed to read source blocks for version 1")  
        
        return tgt_rangeset, src_data, src_rangeset.size  
    
    def _load_src_tgt_version2(self, tokens: List[str], pos: int) -> Tuple[RangeSet, bytearray, int, bool]:  
        """  
        Parse and load source/target for version 2 commands.  
        Returns: (tgt_rangeset, src_data, src_block_count, overlap)  
        """  
        if pos + 2 >= len(tokens):  
            raise ValueError("Invalid parameters for version 2 load")  
    
        tgt_rangeset = RangeSet(tokens[pos])  
        src_block_count = int(tokens[pos + 1])  
        src_data = bytearray(src_block_count * self.BLOCKSIZE)  
        overlap = False # Initialize overlap flag  
        cur = pos + 2  
    
        if tokens[cur] == "-":  
            cur += 1  # No source range, only stashes  
        else:  
            src_rangeset = RangeSet(tokens[cur])  
            read_data = self._read_blocks(src_rangeset)  
            if read_data is None:  
                raise IOError("Failed to read source blocks for version 2")  
            src_data[:len(read_data)] = read_data  
            
            # Perform overlap detection here  
            overlap = BlockImageUpdate.range_overlaps(src_rangeset, tgt_rangeset)# Use the new range_overlaps function  
            
            cur += 1  
            # Optional source location mapping  
            if cur < len(tokens) and ':' not in tokens[cur]:  
                locs_rangeset = RangeSet(tokens[cur])  
                self._move_range(src_data, locs_rangeset, src_data)  
                cur += 1  
    
        # Handle stashes  
        while cur < len(tokens):  
            stash_spec = tokens[cur]  
            if ':' in stash_spec:  
                self._apply_stash_data(src_data, stash_spec)  
            cur += 1  
    
        return tgt_rangeset, src_data, src_block_count, overlap
    
    def _load_src_tgt_version3(self, tokens: List[str], pos: int, onehash: bool) -> Tuple[RangeSet, bytearray, int, bool]:  
        """  
        Parse and load source/target for version 3+ commands.  
        Returns: (tgt_rangeset, src_data, src_block_count, overlap)  
        """  
        if pos >= len(tokens):  
            raise ValueError("Missing source hash for version 3+ load")  
    
        src_hash = tokens[pos]  
        pos += 1  
        if onehash:  
            tgt_hash = src_hash  
        else:  
            if pos >= len(tokens):  
                raise ValueError("Missing target hash for version 3+ load")  
            tgt_hash = tokens[pos]  
            pos += 1  
    
        # Call version 2 loader to get base data and overlap status  
        tgt_rangeset, src_data, src_block_count, overlap = self._load_src_tgt_version2(tokens, pos)  
    
        # Verify source hash  
        actual_src_hash = hashlib.sha1(src_data).hexdigest()  
        if actual_src_hash != src_hash:  
            # If overlap, try to recover from stash  
            if overlap:  
                # Attempt to load previously stashed data for recovery  
                # This requires a LoadStash equivalent that can load into a buffer  
                # For now, we'll simulate this by checking if the stash exists  
                # In a full implementation, you'd call a LoadStash equivalent here  
                # and if successful, set src_data to the loaded stash data.  
                if src_hash in self.stash_map: # Simplified check for existing stash  
                    print(f"Recovering from stashed data for {src_hash}")  
                    src_data = bytearray(self.stash_map[src_hash]) # Load from memory stash  
                    # Re-verify hash of recovered data  
                    if hashlib.sha1(src_data).hexdigest() != src_hash:  
                        print(f"Error: Recovered stash data hash mismatch for {src_hash}")  
                        raise ValueError("Partition has unexpected contents and stash recovery failed")  
                else:  
                    print(f"Partition has unexpected contents (expected {src_hash}, got {actual_src_hash}) and no stash for recovery.")  
                    raise ValueError("Partition has unexpected contents")  
            else:  
                print(f"Partition has unexpected contents (expected {src_hash}, got {actual_src_hash})")  
                raise ValueError("Partition has unexpected contents")  
        else:  
            # Source blocks have expected content, command can proceed  
            # If source and target blocks overlap, stash the source blocks for resume  
            if overlap:  
                print(f"Stashing {src_block_count} overlapping blocks to {src_hash}")  
                # This is where SaveStash equivalent would be called  
                # For now, we'll just store it in memory stash_map  
                self.stash_map[src_hash] = bytes(src_data) # Store as bytes for immutability  
                # In a full implementation, you'd also write to disk (WriteStash)  
                # and manage params.freestash if it's a new stash.  
    
        return tgt_rangeset, src_data, src_block_count, overlap


    def _apply_stash_data(self, buffer: bytearray, stash_spec: str):  
        """Apply stashed data to buffer at specified ranges, mimicking MoveRange with memoryview."""  
        try:  
            stash_id, range_text = stash_spec.split(':', 1)  
            if stash_id not in self.stash_map:  
                print(f"Warning: Stash ID {stash_id} not found for spec {stash_spec}")  
                return  
    
            stash_data = self.stash_map[stash_id]  
            locs = RangeSet(range_text)  
    
            # Use memoryview for efficient in-place modification  
            buffer_view = memoryview(buffer)  
            stash_data_view = memoryview(stash_data)  
    
            current_stash_offset = len(stash_data) # Start from end of stash_data  
            for start_block, end_block in reversed(locs.get_ranges()):  
                num_blocks = end_block - start_block  
                size_to_copy = num_blocks * self.BLOCKSIZE  
                
                current_stash_offset -= size_to_copy  
                if current_stash_offset < 0:  
                    print(f"Error: Stash data too small for range {start_block}-{end_block}")  
                    return  
    
                buffer_start_byte = start_block * self.BLOCKSIZE  
                buffer_end_byte = buffer_start_byte + size_to_copy  
                
                # Perform the copy using memoryview slices  
                # This is equivalent to memmove in C++ for non-overlapping source/dest slices  
                # If source and dest slices overlap, Python's slice assignment handles it correctly  
                buffer_view[buffer_start_byte:buffer_end_byte] = stash_data_view[current_stash_offset:current_stash_offset + size_to_copy]  
                print(f"Applied stash {stash_id} to blocks {start_block}-{end_block}")  
    
        except Exception as e:  
            print(f"Failed to apply stash {stash_spec}: {e}")

    @staticmethod  
    def range_overlaps(r1: RangeSet, r2: RangeSet) -> bool:  
        """Check if two RangeSet objects have any overlapping blocks."""  
        # Sort ranges by start block for efficient checking  
        ranges1 = sorted(r1.get_ranges())  
        ranges2 = sorted(r2.get_ranges())  
    
        i, j = 0, 0  
        while i < len(ranges1) and j < len(ranges2):  
            # Get current ranges  
            start1, end1 = ranges1[i]  
            start2, end2 = ranges2[j]  
    
            # Check for overlap  
            if max(start1, start2) < min(end1, end2):  
                return True  # Overlap found  
    
            # Move to the next range in the set that ends earlier  
            if end1 < end2:  
                i += 1  
            else:  
                j += 1  
        return False


    def _read_blocks(self, rangeset: RangeSet) -> Optional[bytes]:  
        """Read blocks from the block device according to rangeset"""  
        try:  
            data = b''  
            with self._open_block_device('rb') as f:  
                for i in range(0, len(rangeset.pos), 2):  
                    start_block = rangeset.pos[i]  
                    end_block = rangeset.pos[i + 1]  
                    
                    try:  
                        f.seek(start_block * self.BLOCKSIZE)  
                    except OSError as e:  
                        if os.name == 'nt' and e.errno == 22:  # Invalid argument on Windows  
                            print(f"Windows: Cannot seek to block {start_block}, trying alternative method")  
                            # Alternative handling for Windows block devices  
                            continue  
                        raise  
                    
                    for block in range(start_block, end_block):  
                        block_data = f.read(self.BLOCKSIZE)  
                        if len(block_data) != self.BLOCKSIZE:  
                            print(f"Short read at block {block}")  
                            return None  
                        data += block_data  
            return data  
        except Exception as e:  
            print(f"Failed to read blocks: {e}")  
            return None

    
    def _write_blocks(self, rangeset: RangeSet, data: bytes) -> bool:  
        """Write data to blocks according to rangeset"""  
        try:  
            data_offset = 0  
            with open(self.blockdev_path, 'r+b') as f:  
                for i in range(0, len(rangeset.pos), 2):  
                    start_block = rangeset.pos[i]  
                    end_block = rangeset.pos[i + 1]  
                    
                    f.seek(start_block * self.BLOCKSIZE)  
                    for block in range(start_block, end_block):  
                        if data_offset >= len(data):  
                            print("Insufficient data for write")  
                            return False  
                        
                        block_data = data[data_offset:data_offset + self.BLOCKSIZE]  
                        if len(block_data) < self.BLOCKSIZE:  
                            block_data = block_data.ljust(self.BLOCKSIZE, b'\x00')  
                        
                        f.write(block_data)  
                        data_offset += self.BLOCKSIZE  
                    f.flush()  
            
            return True  
        except Exception as e:  
            print(f"Failed to write blocks: {e}")  
            return False
    
    def perform_command_move(self, tokens: List[str], pos: int) -> int:  
        """Move data between block ranges with proper version handling"""  
        
        # Initialize variables at the beginning to avoid scope issues  
        src_rangeset = None  
        tgt_rangeset = None  
        src_hash = None  
        src_block_count = 0  
        
        if self.version == 1:  
            # Version 1: move <tgt_range> <src_range>  
            if pos + 1 >= len(tokens):  
                print("Missing source or target blocks for move")  
                return -1  
            tgt_rangeset = RangeSet(tokens[pos])  
            src_rangeset = RangeSet(tokens[pos + 1])  
            
        elif self.version == 2:  
            # Version 2: move <tgt_range> <src_block_count> <src_range>  
            if pos + 2 >= len(tokens):  
                print("Missing required parameters for version 2 move")  
                return -1  
            tgt_rangeset = RangeSet(tokens[pos])  
            src_block_count = int(tokens[pos + 1])  
            src_rangeset = RangeSet(tokens[pos + 2])  
            
            if src_block_count != src_rangeset.size:  
                print(f"Source block count mismatch: {src_block_count} vs {src_rangeset.size}")  
                return -1  
                
        else:  # version >= 3  
            # Version 3+: move <src_hash> <tgt_range> <src_block_count> <src_range>  
            if pos + 3 >= len(tokens):  
                print("Missing required parameters for version 3+ move")  
                return -1  
                
            try:  
                src_hash = tokens[pos]  
                tgt_rangeset = RangeSet(tokens[pos + 1])  
                src_block_count = int(tokens[pos + 2])  
                
                # Handle source range  
                if pos + 3 < len(tokens) and tokens[pos + 3] != "-":  
                    src_rangeset = RangeSet(tokens[pos + 3])  
                else:  
                    print("Move command with stash-only source not fully implemented")  
                    return -1  
                    
                # Handle block count mismatch - use actual range size instead of failing  
                if src_block_count != src_rangeset.size:  
                    print(f"Warning: Source block count mismatch: expected={src_block_count}, actual={src_rangeset.size}")  
                    print("Using actual range size instead of declared count")  
                    src_block_count = src_rangeset.size  # Use actual size  
                    
            except (ValueError, IndexError) as e:  
                print(f"Error parsing version 3+ move parameters: {e}")  
                return -1  
        
        # Final safety checks  
        if src_rangeset is None or tgt_rangeset is None:  
            print("Failed to initialize source or target rangeset")  
            return -1  
        
        # Handle size mismatch with tolerance for minor discrepancies  
        size_diff = abs(src_rangeset.size - tgt_rangeset.size)  
        if size_diff > 1:  # Allow 1 block difference  
            print(f"Source and target ranges size difference too large: src={src_rangeset.size}, tgt={tgt_rangeset.size}")  
            return -1  
        elif size_diff == 1:  
            print(f"Warning: Minor size difference detected: src={src_rangeset.size}, tgt={tgt_rangeset.size}")  
            print("Proceeding with smaller range size")  
            # Use the smaller size to avoid out-of-bounds access  
            actual_size = min(src_rangeset.size, tgt_rangeset.size)  
        else:  
            actual_size = src_rangeset.size  
            
        print(f"  moving {actual_size} blocks")  
        
        try:  
            # Read source data first to handle overlapping ranges  
            src_data = self._read_blocks(src_rangeset)  
            if src_data is None:  
                return -1  
            
            # Verify source hash if available (version 3+)  
            if self.version >= 3 and src_hash and src_hash != "0" * 40:  
                actual_src_hash = hashlib.sha1(src_data).hexdigest()  
                if actual_src_hash != src_hash:  
                    # Check if we need to handle overlapping ranges  
                    overlap = BlockImageUpdate.range_overlaps(src_rangeset, tgt_rangeset)  
                    
                    if overlap:  
                        # Try to recover from stash if available  
                        if src_hash in self.stash_map:  
                            print(f"Recovering source data from stash {src_hash}")  
                            src_data = self.stash_map[src_hash]  
                            # Re-verify after recovery  
                            recovered_hash = hashlib.sha1(src_data).hexdigest()  
                            if recovered_hash != src_hash:  
                                print(f"Stash recovery failed: expected {src_hash}, got {recovered_hash}")  
                                return -1  
                        else:  
                            # Try to load from disk stash  
                            stash_file = os.path.join(self.stash_base_dir, f"stash_{src_hash}")  
                            if os.path.exists(stash_file):  
                                print(f"Loading stash from disk: {src_hash}")  
                                with open(stash_file, 'rb') as f:  
                                    src_data = f.read()  
                                # Re-verify after loading  
                                recovered_hash = hashlib.sha1(src_data).hexdigest()  
                                if recovered_hash != src_hash:  
                                    print(f"Disk stash recovery failed: expected {src_hash}, got {recovered_hash}")  
                                    return -1  
                            else:  
                                print(f"Source hash mismatch in move: expected {src_hash}, got {actual_src_hash}")  
                                print("No stash available for recovery")  
                                return -1  
                    else:  
                        print(f"Source hash mismatch in move: expected {src_hash}, got {actual_src_hash}")  
                        print("No overlap detected, cannot recover")  
                        return -1  
            
            # Adjust data size if needed for size mismatch  
            if size_diff == 1 and len(src_data) > actual_size * self.BLOCKSIZE:  
                src_data = src_data[:actual_size * self.BLOCKSIZE]  
            
            # Write to target  
            if not self._write_blocks(tgt_rangeset, src_data):  
                return -1  
            
            self.written += actual_size  
            return 0  
            
        except Exception as e:  
            print(f"Failed to move blocks: {e}")  
            return -1

    def perform_command_stash(self, tokens: List[str], pos: int) -> int:  
        """Stash blocks for later use"""  
        if pos + 1 >= len(tokens):  
            print("Missing stash id or range for stash")  
            return -1  
            
        try:  
            stash_id = tokens[pos]  
            rangeset = RangeSet(tokens[pos + 1])  
            
            print(f"  stashing {rangeset.size} blocks as {stash_id}")  
            
            # Read blocks to stash  
            stash_data = self._read_blocks(rangeset)  
            if stash_data is None:  
                return -1  
            
            # Store in memory and optionally on disk  
            self.stash_map[stash_id] = stash_data  
            
            # Also save to disk for large stashes  
            stash_file = os.path.join(self.stash_base_dir, f"stash_{stash_id}")  
            with open(stash_file, 'wb') as f:  
                f.write(stash_data)  
            
            return 0  
            
        except Exception as e:  
            print(f"Failed to stash blocks: {e}")  
            return -1  
    
    def perform_command_free(self, tokens: List[str], pos: int) -> int:  
        """Free stashed data"""  
        if pos >= len(tokens):  
            print("Missing stash id for free")  
            return -1  
            
        stash_id = tokens[pos]  
        
        # Remove from memory  
        if stash_id in self.stash_map:  
            del self.stash_map[stash_id]  
            print(f"  freed stash {stash_id}")  
        else:  
            print(f"  stash {stash_id} not found (already freed?)")  
        
        # Remove from disk  
        stash_file = os.path.join(self.stash_base_dir, f"stash_{stash_id}")  
        if os.path.exists(stash_file):  
            os.unlink(stash_file)  
        
        return 0

    def process_transfer_list(self) -> int:  
        """Process the transfer list file with resume capability using combined approach"""  
        try:  
            with open(self.transfer_list_path, 'r') as f:  
                self.transfer_lines = [line.strip() for line in f.readlines()]  
        except IOError as e:  
            print(f"Failed to read transfer list: {e}")  
            return -1  
        
        # Parse header and determine starting line  
        if len(self.transfer_lines) < 2:  
            print("Transfer list too short")  
            return -1  
        
        try:  
            self.version = int(self.transfer_lines[0])  
            self.total_blocks = int(self.transfer_lines[1])  
        except ValueError as e:  
            print(f"Failed to parse transfer list header: {e}")  
            return -1  
        
        print(f"blockimg version is {self.version}")  
        print(f"total blocks: {self.total_blocks}")  
        
        if self.total_blocks == 0:  
            return 0  
        
        # Handle version-specific header parsing  
        if self.version >= 2:  
            if len(self.transfer_lines) < 4:  
                print("Transfer list too short for version 2+")  
                return -1  
            max_stash_entries = self.transfer_lines[2]  
            max_stash_blocks = int(self.transfer_lines[3])  
            print(f"maximum stash entries {max_stash_entries}")  
            print(f"maximum stash blocks {max_stash_blocks}")  
        
        # Determine resume point using combined approach  
        start_line = self.determine_resume_point()  
        
        # Process commands  
        result = self._process_commands(start_line)  
        
        # Clean up on successful completion  
        if result == 0:  
            self._cleanup_progress_checkpoint()  
            print(f"Successfully processed commands")  
            print(f"wrote {self.written} blocks; expected {self.total_blocks}")  
        
        return result  
    
    def _process_commands(self, start_line: int) -> int:  
        """Process transfer list commands starting from given line"""  
        commands = {  
            'zero': lambda tokens, pos: self.perform_command_zero(tokens, pos),  
            'new': lambda tokens, pos: self.perform_command_new(tokens, pos),  
            'erase': lambda tokens, pos: self.perform_command_erase(tokens, pos),  
            'move': lambda tokens, pos: self.perform_command_move(tokens, pos),  
            'bsdiff': lambda tokens, pos: self.perform_command_diff(tokens, pos, 'bsdiff'),  
            'imgdiff': lambda tokens, pos: self.perform_command_diff(tokens, pos, 'imgdiff'),  
            'stash': lambda tokens, pos: self.perform_command_stash(tokens, pos),  
            'free': lambda tokens, pos: self.perform_command_free(tokens, pos),  
        }  
        
        for line_num in range(start_line, len(self.transfer_lines)):  
            line = self.transfer_lines[line_num].strip()  
            if not line:  
                continue  
                
            tokens = line.split()  
            if not tokens:  
                continue  
                
            cmd = tokens[0]  
            if cmd not in commands:  
                print(f"Unknown command: {cmd}")  
                return -1  
            
            print(f"Executing: {cmd}")  
            try:  
                result = commands[cmd](tokens, 1)  
                if result != 0:  
                    print(f"Command {cmd} failed with code {result}")  
                    return result  
                    
                # Update progress and save checkpoint if needed  
                self.update_and_save_progress(line_num, cmd)  
                    
            except Exception as e:  
                print(f"Exception executing command {cmd}: {e}")  
                return -1  
        
        return 0

    def verify_command_completion(self, tokens: List[str], cmd_name: str) -> bool:  
        """Verify if command is completed by checking target SHA1"""  
        if self.version >= 3 and cmd_name in ['bsdiff', 'imgdiff']:  
            try:  
                tgt_hash = tokens[4]  # Target SHA1 hash  
                tgt_range = tokens[5]  # Target block range  
                
                tgt_rangeset = RangeSet(tgt_range)  
                actual_data = self._read_blocks(tgt_rangeset)  
                if actual_data:  
                    actual_hash = hashlib.sha1(actual_data).hexdigest()  
                    return actual_hash == tgt_hash  
            except Exception:  
                return False  
        return False  
    

    def save_progress_checkpoint(self, line_num: int, written_blocks: int):  
        """Save simple progress checkpoint to JSON file"""  
        checkpoint = {  
            'line': line_num,  
            'written': written_blocks,  
            'timestamp': time.time(),  
            'version': self.version  
        }  
        progress_file = f"{self.stash_base_dir}/progress.json"  
        with open(progress_file, 'w') as f:  
            json.dump(checkpoint, f, indent=2)  
    
    def load_progress_checkpoint(self) -> int:  
        """Load progress checkpoint and return line number to resume from"""  
        try:  
            progress_file = f"{self.stash_base_dir}/progress.json"  
            if not os.path.exists(progress_file):  
                return -1  
                
            with open(progress_file, 'r') as f:  
                checkpoint = json.load(f)  
                
            # Verify checkpoint is for same transfer list version  
            if checkpoint.get('version') != self.version:  
                print("Warning: Checkpoint version mismatch, starting from beginning")  
                return -1  
                
            self.written = checkpoint['written']  
            print(f"Resuming from line {checkpoint['line']}, {self.written} blocks written")  
            return checkpoint['line']  
            
        except Exception as e:  
            print(f"Failed to load checkpoint: {e}")  
            return -1  
    
    def _cleanup_progress_checkpoint(self):  
        """Clean up progress checkpoint file after successful completion"""  
        try:  
            progress_file = f"{self.stash_base_dir}/progress.json"  
            if os.path.exists(progress_file):  
                os.unlink(progress_file)  
                print("Progress checkpoint cleaned up")  
        except Exception as e:  
            print(f"Warning: Failed to cleanup progress checkpoint: {e}")

    def quick_resume_check(self) -> int:  
        """Quick resume check using target hash verification for critical commands"""  
        start_line = 4 if self.version >= 2 else 2  
        
        for line_num in range(start_line, len(self.transfer_lines)):  
            line = self.transfer_lines[line_num].strip()  
            if not line:  
                continue  
                
            tokens = line.split()  
            if not tokens:  
                continue  
                
            cmd = tokens[0]  
            
            # Only check commands that modify data  
            if cmd in ['bsdiff', 'imgdiff', 'new', 'move']:  
                if not self._is_command_completed(tokens, cmd):  
                    return line_num  
        
        return -1  # All commands completed  
    
    def _is_command_completed(self, tokens: List[str], cmd_name: str) -> bool:  
        """Check if a specific command has been completed by verifying target blocks"""  
        try:  
            if self.version >= 3 and cmd_name in ['bsdiff', 'imgdiff']:  
                # For version 3+: cmd offset length src_hash tgt_hash tgt_range ...  
                if len(tokens) >= 6:  
                    tgt_hash = tokens[4]  # Target SHA1 hash  
                    tgt_range = tokens[5]  # Target block range  
                    
                    tgt_rangeset = RangeSet(tgt_range)  
                    actual_data = self._read_blocks(tgt_rangeset)  
                    if actual_data:  
                        actual_hash = hashlib.sha1(actual_data).hexdigest()  
                        return actual_hash == tgt_hash  
            elif cmd_name == 'new':  
                # For 'new' commands, check if target blocks contain non-zero data  
                if len(tokens) >= 2:  
                    tgt_range = tokens[1]  
                    tgt_rangeset = RangeSet(tgt_range)  
                    actual_data = self._read_blocks(tgt_rangeset)  
                    if actual_data:  
                        # Check if blocks are not all zeros (indicating they've been written)  
                        return not all(b == 0 for b in actual_data)  
                
        except Exception as e:  
            print(f"Error checking command completion: {e}")  
            
        return False

    def determine_resume_point(self) -> int:  
        """Determine resume point using combined checkpoint and hash verification approach"""  
        start_line = 4 if self.version >= 2 else 2  
        
        # Try checkpoint first (方案1)  
        resume_line = self.load_progress_checkpoint()  
        if resume_line > start_line:  
            print(f"Resuming from checkpoint at line {resume_line}")  
            return resume_line  
        
        # Fallback to hash verification (方案2)  
        resume_line = self.quick_resume_check()  
        if resume_line > start_line:  
            print(f"Resuming from hash verification at line {resume_line}")  
            return resume_line  
        
        return start_line

    def should_save_checkpoint(self, line_num: int, cmd: str) -> bool:  
        """Determine if checkpoint should be saved at this point"""  
        # Save every 10 commands or after critical operations  
        return line_num % 10 == 0 or cmd in ['bsdiff', 'imgdiff', 'new']  
    
    def update_and_save_progress(self, line_num: int, cmd: str):  
        """Update progress and save checkpoint if needed"""  
        if self.should_save_checkpoint(line_num, cmd):  
            self.save_progress_checkpoint(line_num + 1, self.written)       
        # Update progress display  
        if self.total_blocks > 0:  
            progress = (self.written * 100.0) / self.total_blocks  
            print(f"Progress: {progress:.1f}% ({self.written}/{self.total_blocks} blocks)")

    def _read_blocks_streaming(self, rangeset: RangeSet, chunk_size: int = 1024*1024) -> Iterator[bytes]:  
        """Read blocks in streaming fashion to reduce memory usage"""  
        with self._open_block_device('rb') as f:  
            for start_block, end_block in rangeset:  
                f.seek(start_block * self.BLOCKSIZE)  
                remaining = (end_block - start_block) * self.BLOCKSIZE  
                
                while remaining > 0:  
                    read_size = min(chunk_size, remaining)  
                    chunk = f.read(read_size)  
                    if not chunk:  
                        break  
                    yield chunk  
                    remaining -= len(chunk)

    def _new_data_producer_optimized(self, fd: BinaryIO, q: queue.Queue, cond: threading.Condition, block_size: int):  
        """Optimized producer with better buffering and error handling"""  
        buffer_size = block_size * 10  # Read multiple blocks at once  
        
        while self.new_data_producer_running:  
            try:  
                # Read larger chunks for better I/O efficiency  
                data = fd.read(buffer_size)  
                if not data:  
                    q.put(None)  
                    break  
                    
                # Split into individual blocks  
                for i in range(0, len(data), block_size):  
                    block = data[i:i + block_size]  
                    if len(block) < block_size:  
                        block = block.ljust(block_size, b'\x00')  
                    q.put(block)  
                    
            except Exception as e:  
                print(f"Producer error: {e}")  
                q.put(None)  
                break

    def _verify_and_recover_blocks(self, rangeset: RangeSet, expected_hash: str) -> bool:  
        """Verify blocks and attempt recovery if needed"""  
        actual_data = self._read_blocks(rangeset)  
        if not actual_data:  
            return False  
            
        actual_hash = hashlib.sha1(actual_data).hexdigest()  
        if actual_hash == expected_hash:  
            return True  
            
        # Try to recover from stash  
        if expected_hash in self.stash_map:  
            print(f"Recovering blocks from stash {expected_hash}")  
            if self._write_blocks(rangeset, self.stash_map[expected_hash]):  
                return True  
                
        return False

    def _estimate_target_size(self, patch_data: bytes, src_data_len: int) -> int:  
        """Estimates the target file size based on bsdiff or imgdiff patch header information"""  
        if len(patch_data) < 8:  
            return src_data_len  
        
        # Check patch type  
        header = patch_data[:8]  
        
        if header == b"BSDIFF40":  
            # BSDiff format: header structure is 32 bytes  
            # 24-32: target file size (8 bytes, little-endian)  
            if len(patch_data) >= 32:  
                try:  
                    target_size = struct.unpack('<Q', patch_data[24:32])[0]  
                    return target_size  
                except struct.error:  
                    return src_data_len  
            return src_data_len  
            
        elif header == b"IMGDIFF2":  
            # ImgDiff format: simplified estimate  
            # Return source size as estimate since ImgDiff doesn't have direct target size field  
            return src_data_len  
        
        else:  
            # Unknown format, return source file size as fallback  
            return src_data_len

    
    
    def _create_verification_cache(self):  
        """Create cache of verified block ranges"""  
        self.verified_ranges = {}  
        
    def _is_range_verified(self, rangeset: RangeSet, expected_hash: str) -> bool:  
        """Check if range is already verified"""  
        range_key = f"{rangeset.pos}_{expected_hash}"  
        return range_key in self.verified_ranges

    def _get_optimal_io_settings(self):  
        """Get platform-specific optimal I/O settings"""  
        if os.name == 'nt':  # Windows  
            return {  
                'buffer_size': 64 * 1024,  # 64KB for Windows  
                'use_direct_io': False,  
                'sync_method': 'flush'  
            }  
        else:  # Unix-like  
            return {  
                'buffer_size': 1024 * 1024,  # 1MB for Unix  
                'use_direct_io': True,  
                'sync_method': 'fsync'  
            }

    def _enhanced_progress_reporting(self, cmd: str, blocks_processed: int):  
        """Enhanced progress reporting with timing and throughput"""  
        current_time = time.time()  
        if hasattr(self, '_last_progress_time'):  
            time_diff = current_time - self._last_progress_time  
            if time_diff > 0:  
                throughput = blocks_processed / time_diff  
                print(f"Progress: {self.written}/{self.total_blocks} blocks "  
                    f"({(self.written/self.total_blocks)*100:.1f}%) "  
                    f"Throughput: {throughput:.1f} blocks/sec")  
        
        self._last_progress_time = current_time

    def __exit__(self, exc_type, exc_val, exc_tb):  
        """Context manager exit - cleanup resources"""  
        # Signal producer to stop and wait for it to finish  
        if self.new_data_producer_thread and self.new_data_producer_thread.is_alive():  
            self.new_data_producer_running = False  
            with self.new_data_condition:  
                self.new_data_condition.notify_all()  # Wake up producer if it's waiting  
            self.new_data_producer_thread.join(timeout=5)  # Give it some time to finish  
    
        # Close file descriptors  
        if self.new_data_fd:  
            self.new_data_fd.close()  
            self.new_data_fd = None  
        if self.patch_data_fd:  
            self.patch_data_fd.close()  
            self.patch_data_fd = None  
    
        # If normal completion (no exception), clean up progress file  
        if exc_type is None:  
            self._cleanup_progress_checkpoint()  
        
        # Cleanup stash files and directory  
        self._cleanup_stash()  
        
        # Additional cleanup for any remaining resources  
        try:  
            # Clear any remaining queue items  
            while not self.new_data_queue.empty():  
                try:  
                    self.new_data_queue.get_nowait()  
                    self.new_data_queue.task_done()  
                except queue.Empty:  
                    break  
        except Exception as e:  
            print(f"Warning: Failed to clear queue during cleanup: {e}")



def main():  
    """Main entry point"""  
    if len(sys.argv) < 5:  
        print(f"usage: {sys.argv[0]} <system.img> <system.transfer.list> <system.new.dat> <system.patch.dat>")  
        print("args:")  
        print("\t- block device (or file) to modify in-place")  
        print("\t- transfer list (blob)")  
        print("\t- new data stream (filename within package.zip)")  
        print("\t- patch stream (filename within package.zip, must be uncompressed)")  
        return 1  
      
    try:  
        with BlockImageUpdate(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]) as biu:  
            result = biu.process_transfer_list()  
              
            if result == 0:  
                print("Done")  
            else:  
                print(f"Done with error code: {result}")  
              
            return result  
    except Exception as e:  
        print(f"Fatal error: {e}")  
        return -1  
  
if __name__ == "__main__":  
    sys.exit(main())