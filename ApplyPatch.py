#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import hashlib
import struct
import tempfile
import shutil
import time
import zlib
import asyncio
import bsdiff4
import mmap
import logging
from pathlib import Path, PurePath
from concurrent.futures import ProcessPoolExecutor
from contextlib import contextmanager
from typing import List, Optional
from io import BytesIO
import bz2
from zipfile import ZipFile

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # or INFO in production

# ---------------------------------------------------------------------------
# Constants for IMGDIFF2
# ---------------------------------------------------------------------------
CHUNK_NORMAL   = 0
CHUNK_GZIP     = 1
CHUNK_DEFLATE  = 2
CHUNK_RAW      = 3

# ---------------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------------
def ensure_bytes(data) -> bytes:
    if isinstance(data, (bytes, bytearray)):
        return data
    if hasattr(data, "tobytes"):
        return data.tobytes()
    if hasattr(data, "__getitem__"):
        return bytes(data)
    raise TypeError(f"Cannot convert {type(data)} to bytes")

def parse_sha1(sha1_str: str) -> bytes:
    """Parse SHA1 hex (possibly with trailing :... suffix) into bytes."""
    part = sha1_str.split(":", 1)[0]
    if len(part) != 40:
        raise ValueError(f"Invalid SHA1 length: {len(part)}")
    return bytes.fromhex(part)

def short_sha1(sha1_bytes: bytes) -> str:
    return sha1_bytes.hex()[:8]

def detect_compression_type(data: bytes) -> str:
    """
    - BZh…       → 'bzip2'
    - PK\x03\x04 → 'deflate_zip'
    - 其他       → 'raw_deflate'
    """
    if data.startswith(b"BZh"):
        return "bzip2"
    if data.startswith(b"PK\x03\x04"):
        return "deflate_zip"
    return "raw_deflate"

def decompress_data(data: bytes) -> bytes:
    """
    根据魔数自动解压：
      - bzip2
      - zip(local-file-header)+deflate
      - raw deflate (wbits=-MAX_WBITS)
    """
    ctype = detect_compression_type(data)
    if ctype == "bzip2":
        return bz2.decompress(data)
    if ctype == "deflate_zip":
        with ZipFile(BytesIO(data)) as zf:
            return zf.read(zf.infolist()[0])
    # raw deflate 回退
    return zlib.decompress(data, -zlib.MAX_WBITS)

# ---------------------------------------------------------------------------
# FileContents & loading
# ---------------------------------------------------------------------------
class FileContents:
    __slots__ = ("data", "filename", "sha1", "st")
    def __init__(self, data: bytes, filename: str):
        self.data = data
        self.filename = filename
        self.sha1 = hashlib.sha1(data).digest()
        # Windows-safe stat
        path = Path(filename)
        for _ in range(3):
            try:
                self.st = path.stat()
                break
            except OSError as e:
                if os.name == "nt" and hasattr(e, "winerror") and e.winerror == 6:
                    time.sleep(0.1)
                    continue
                raise

def load_file_contents_mmap(filename: str) -> FileContents:
    path = Path(filename)
    if not path.exists():
        raise FileNotFoundError(filename)
    size = path.stat().st_size
    if size == 0:
        return FileContents(b"", filename)
    if size < 1 << 20:
        with open(filename, "rb") as f:
            data = f.read()
    else:
        try:
            with open(filename, "rb") as f:
                mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
                data = mm[:]
                mm.close()
        except Exception:
            with open(filename, "rb") as f:
                data = f.read()
    return FileContents(data, filename)

def load_file_contents(filename: str) -> FileContents:
    return load_file_contents_mmap(filename)

def save_file_contents(filename: str, fc: FileContents) -> bool:
    try:
        p = Path(filename)
        p.parent.mkdir(parents=True, exist_ok=True)
        with open(filename, "wb") as f:
            f.write(fc.data)
        # restore permissions
        try:
            os.chmod(filename, fc.st.st_mode)
            if os.name != "nt":
                os.chown(filename, fc.st.st_uid, fc.st.st_gid)
        except Exception:
            pass
        return True
    except Exception as e:
        logger.error(f"Failed to save {filename}: {e}", exc_info=True)
        return False

def find_matching_patch(file_sha1: bytes, patch_sha1_list: List[str]) -> int:
    for i, s in enumerate(patch_sha1_list):
        try:
            if parse_sha1(s) == file_sha1:
                return i
        except Exception:
            continue
    return -1

# ---------------------------------------------------------------------------
# Cache directory manager
# ---------------------------------------------------------------------------
def get_cache_temp_source() -> Path:
    if os.name == "nt":
        base = Path(os.environ.get("TEMP", os.environ.get("TMP", "C:\\temp")))
        cache = base / "applypatch_cache"
    else:
        cache = Path("/tmp/applypatch_cache")
    for _ in range(3):
        try:
            cache.mkdir(parents=True, exist_ok=True)
            break
        except Exception:
            time.sleep(0.1)
    return cache / "temp_source"

CACHE_TEMP_SOURCE = get_cache_temp_source()

@contextmanager
def cache_manager():
    cache_dir = CACHE_TEMP_SOURCE.parent
    cache_dir.mkdir(parents=True, exist_ok=True)
    try:
        yield cache_dir
    finally:
        try:
            if CACHE_TEMP_SOURCE.exists():
                CACHE_TEMP_SOURCE.unlink()
            cache_dir.rmdir()
        except Exception:
            shutil.rmtree(cache_dir, ignore_errors=True)

# ---------------------------------------------------------------------------
# Atomic file write
# ---------------------------------------------------------------------------
def write_file_atomic(target_path: Path, data: bytes, meta: FileContents):
    dir_ = target_path.parent
    dir_.mkdir(parents=True, exist_ok=True)
    tmpf = tempfile.NamedTemporaryFile(delete=False, dir=str(dir_), suffix=".tmp")
    tmpf.write(data)
    tmpf.flush(); os.fsync(tmpf.fileno())
    tmpf.close()
    # permissions
    os.chmod(tmpf.name, meta.st.st_mode)
    if os.name != 'nt':
        os.chown(tmpf.name, meta.st.st_uid, meta.st.st_gid)
    # replace
    os.replace(tmpf.name, str(target_path))

# ---------------------------------------------------------------------------
# BSDIFF4 wrapper
# ---------------------------------------------------------------------------
def apply_bsdiff_patch(src: bytes, patch: bytes) -> bytes:
    if not patch.startswith(b"BSDIFF40"):
        raise ValueError("Not a BSDIFF40 patch")
    return bsdiff4.patch(src, patch)

# ---------------------------------------------------------------------------
# Corrected IMGDIFF2 streaming patch
# ---------------------------------------------------------------------------
def apply_imgdiff_patch_streaming(src_data: bytes,
                                  patch_data: bytes,
                                  bonus_data: Optional[bytes] = None) -> bytes:
    """
    Apply an IMGDIFF2-style streaming patch.
    Supports CHUNK_NORMAL,CHUNK_DEFLATE, CHUNK_RAW.
    Uses decompress_data() for deflate/gzip with raw-deflate fallback.
    """
    # short‐hands
    src = src_data
    pd  = patch_data

    # header + chunk count
    if not pd.startswith(b"IMGDIFF2"):
        raise ValueError("Not a valid IMGDIFF2 patch")
    num_chunks = struct.unpack_from("<I", pd, 8)[0]
    pos = 12

    # parse chunk headers
    headers = []
    for _ in range(num_chunks):
        ctype = struct.unpack_from("<I", pd, pos)[0]
        pos += 4

        if ctype == CHUNK_NORMAL:
            s0, cnt, po = struct.unpack_from("<QQQ", pd, pos)
            pos += 24
            headers.append({
                "type":       "normal",
                "src_start":  s0,
                "src_len":    cnt,
                "patch_off":  po
            })

        elif ctype in (CHUNK_GZIP, CHUNK_DEFLATE):
            s0, cnt, po, exp_len = struct.unpack_from("<QQQQ", pd, pos)
            pos += 32

            cmp_params = None
            if ctype == CHUNK_DEFLATE:
                lvl, _, wb, ml, strat = struct.unpack_from("<5I", pd, pos)
                pos += 20
                cmp_params = (lvl, wb, ml, strat)

            headers.append({
                "type":        "deflate",
                "chunk_type":  ctype,
                "src_start":   s0,
                "src_len":     cnt,
                "patch_off":   po,
                "exp_len":     exp_len,
                "cmp_params":  cmp_params
            })

        elif ctype == CHUNK_RAW:
            raw_len = struct.unpack_from("<I", pd, pos)[0]
            pos += 4
            headers.append({
                "type":    "raw",
                "raw_len": raw_len
            })

        else:
            raise ValueError(f"Unknown chunk type {ctype}")

    # after headers, payload data begins
    payload_base = pos

    # build end‐offset map for compressed blocks
    offsets = sorted(
        {h["patch_off"] for h in headers if h["type"] == "deflate"} |
        {len(pd)}
    )
    end_map = {offsets[i]: offsets[i+1] for i in range(len(offsets)-1)}

    result = bytearray()

    # process each chunk
    for h in headers:
        kind = h["type"]

        if kind == "normal":
            s0, cnt, po = h["src_start"], h["src_len"], h["patch_off"]
            src_blk   = src[s0:s0+cnt]
            start, end= po, end_map.get(po, po)
            patch_blk = pd[start:end]
            result.extend(apply_bsdiff_patch(src_blk, patch_blk))

        elif kind == "deflate":
            s0, cnt, po = h["src_start"], h["src_len"], h["patch_off"]
            exp_len     = h["exp_len"]
            cmp_params  = h["cmp_params"]
            cty         = h["chunk_type"]

            # extract compressed data from source
            comp_src = src[s0:s0+cnt]

            # universal decompress (bzip2/zip‐deflate/raw‐deflate)
            try:
                uncmp = decompress_data(comp_src)
            except Exception as e:
                logger.error(f"Decompression failed at chunk (deflate): {e}")
                raise

            # pad with bonus_data if too short
            if len(uncmp) < exp_len:
                missing = exp_len - len(uncmp)
                logger.warning(
                    f"Uncompressed data is {missing} bytes short, using bonus_data"
                )
                if bonus_data and len(bonus_data) >= missing:
                    uncmp += bonus_data[:missing]
                else:
                    logger.error("Insufficient bonus_data; patch may be incomplete")

            # apply BSDIFF patch to the uncompressed block
            start, end = po, end_map.get(po, po)
            patched    = apply_bsdiff_patch(uncmp, pd[start:end])

            # recompress according to original chunk type
            if cty == CHUNK_GZIP:
                lvl = cmp_params[0] if cmp_params else 6
                comp = zlib.compress(
                    patched,
                    level=lvl,
                    wbits=16 + zlib.MAX_WBITS
                )
            else:
                lvl, wb, ml, strat = cmp_params or (6, -zlib.MAX_WBITS, 8, 0)
                co   = zlib.compressobj(lvl, zlib.DEFLATED, wb, ml, strat)
                comp = co.compress(patched) + co.flush()

            result.extend(comp)

        else:  # raw
            ln  = h["raw_len"]
            blk = pd[payload_base:payload_base + ln]
            result.extend(blk)
            payload_base += ln

    return bytes(result)
# ---------------------------------------------------------------------------
# Filename sanitization
# ---------------------------------------------------------------------------
def sanitize_filename(filename: str) -> str:
    p = PurePath(filename)
    name = p.stem
    safe = set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-")
    clean = "".join(c if c in safe else "_" for c in name)
    return f"patched_{clean}.img"

# ---------------------------------------------------------------------------
# Patch generation (async/sync)
# ---------------------------------------------------------------------------
def apply_and_verify(data: bytes, patch: bytes,
                     size: int, sha1: bytes,
                     bonus: Optional[bytes] = None) -> bytes:
    if patch.startswith(b"BSDIFF40"):
        out = apply_bsdiff_patch(data, patch)
    elif patch.startswith(b"IMGDIFF2"):
        out = apply_imgdiff_patch_streaming(data, patch, bonus)
    else:
        raise ValueError("Unknown patch format")
    if len(out) != size:
        raise ValueError(f"Size mismatch {len(out)} != {size}")
    actual = hashlib.sha1(out).digest()
    if actual != sha1:
        raise ValueError(f"SHA1 mismatch {actual.hex()} != {sha1.hex()}")
    return out

async def generate_target_async(src: FileContents,
                                patch: bytes,
                                tgt_name: str,
                                tgt_sha1: bytes,
                                tgt_size: int,
                                bonus: Optional[bytes] = None) -> bool:
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    try:
        # run in executor to avoid blocking event loop
        out = await loop.run_in_executor(
            None, apply_and_verify, src.data, patch, tgt_size, tgt_sha1, bonus
        )
        write_file_atomic(Path(tgt_name), out, src)
        return True
    except Exception as e:
        logger.error(f"Async patch failed: {e}", exc_info=True)
        return False

def generate_target_sync(src: FileContents,
                         patch: bytes,
                         tgt_name: str,
                         tgt_sha1: bytes,
                         tgt_size: int,
                         bonus: Optional[bytes] = None) -> bool:
    try:
        out = apply_and_verify(src.data, patch, tgt_size, tgt_sha1, bonus)
        write_file_atomic(Path(tgt_name), out, src)
        return True
    except Exception as e:
        logger.error(f"Sync patch failed: {e}", exc_info=True)
        return False

def generate_target_with_retry(src: FileContents,
                               patch: bytes,
                               tgt_name: str,
                               tgt_sha1: bytes,
                               tgt_size: int,
                               bonus: Optional[bytes] = None) -> bool:
    # prefer async
    try:
        return asyncio.run(generate_target_async(
            src, patch, tgt_name, tgt_sha1, tgt_size, bonus
        ))
    except Exception:
        logger.warning("Falling back to sync")
        return generate_target_sync(src, patch, tgt_name, tgt_sha1, tgt_size, bonus)

# ---------------------------------------------------------------------------
# applypatch command logic
# ---------------------------------------------------------------------------
def applypatch_check(filename: str, patch_sha1_list: List[str]) -> bool:
    try:
        fc = load_file_contents(filename)
        if not patch_sha1_list:
            return True
        return find_matching_patch(fc.sha1, patch_sha1_list) >= 0
    except Exception:
        return False

def applypatch_with_cache(src_name: str, tgt_name: str,
                         tgt_sha1_str: str,
                         tgt_size: int,
                         patch_sha1_list: List[str],
                         patch_files: List[str],
                         bonus: Optional[bytes] = None) -> int:
    with cache_manager():
        print(f"Patching {src_name} → {tgt_name}")
        if tgt_name == "-":
            tgt_name = src_name

        # parse target SHA1
        try:
            tgt_sha1 = parse_sha1(tgt_sha1_str)
        except Exception as e:
            logger.error(f"Bad target SHA1: {e}")
            return 1

        # already patched?
        if os.path.exists(tgt_name):
            try:
                if load_file_contents(tgt_name).sha1 == tgt_sha1:
                    print(f"already {short_sha1(tgt_sha1)}")
                    return 0
            except Exception:
                pass

        # load source
        try:
            src_fc = load_file_contents(src_name)
        except Exception as e:
            logger.error(f"Cannot load source: {e}")
            return 1

        # find patch index
        idx = find_matching_patch(src_fc.sha1, patch_sha1_list)
        # try cache fallback
        if idx < 0:
            if CACHE_TEMP_SOURCE.exists():
                try:
                    cf = load_file_contents(str(CACHE_TEMP_SOURCE))
                    idx = find_matching_patch(cf.sha1, patch_sha1_list)
                    if idx >= 0:
                        src_fc = cf
                        print("using cached source")
                except Exception:
                    pass
        if idx < 0:
            print("source SHA1 not matched")
            return 1

        # backup source to cache
        try:
            CACHE_TEMP_SOURCE.parent.mkdir(parents=True, exist_ok=True)
            save_file_contents(str(CACHE_TEMP_SOURCE), src_fc)
        except Exception as e:
            logger.warning(f"Cache backup failed: {e}")

        # load patch data
        try:
            with open(patch_files[idx], "rb") as f:
                patch_blob = f.read()
        except Exception as e:
            logger.error(f"Cannot read patch file: {e}")
            return 1

        # apply
        ok = generate_target_with_retry(
            src_fc, patch_blob, tgt_name, tgt_sha1, tgt_size, bonus
        )
        if ok:
            print(f"now {short_sha1(tgt_sha1)}")
            return 0
        else:
            return 1

def check_python_version():
    v = sys.version_info
    if v < (3, 8):
        print("Requires Python 3.8+, current:", sys.version)
    elif v >= (3, 13):
        print("Running Python", v.major, v.minor)

def main():
    if len(sys.argv) < 7:
        print("Usage: applypatch.py <src> <tgt> <tgt_sha1> <size> "
              "<init_sha11> <patch1> [init_sha12 patch2 ...] [bonus]")
        return 1

    src_name = sys.argv[1]
    tgt_name = sys.argv[2]
    tgt_sha1  = sys.argv[3]
    try:
        tgt_size = int(sys.argv[4])
    except ValueError:
        print("Bad size:", sys.argv[4])
        return 1

    args = sys.argv[5:]
    bonus = None
    if len(args) % 2 != 0:
        bonus_fname = args[-1]
        args = args[:-1]
        try:
            bonus = open(bonus_fname, "rb").read()
        except Exception as e:
            logger.error(f"Cannot load bonus: {e}")
            return 1

    if len(args) % 2 != 0:
        print("Bad patch arguments")
        return 1

    sha1s, pfiles = [], []
    for i in range(0, len(args), 2):
        sha1s.append(args[i])
        pfiles.append(args[i+1])

    code = applypatch_with_cache(
        src_name, tgt_name, tgt_sha1, tgt_size, sha1s, pfiles, bonus
    )
    if code == 0:
        print("Done")
    else:
        print("Failed with code", code)
    return code

if __name__ == "__main__":
    check_python_version()
    sys.exit(main())