# The AI-Generated [imgpatchtools](https://github.com/erfanoabdi/imgpatchtools)
Patch img files with <Partition>.patch.dat But just needed Python3

## Original Information
- [Original Repository](https://github.com/erfanoabdi/imgpatchtools)
- [Author Page](https://github.com/erfanoabdi)

## License
As the Original Repository,this Repo also use the GPL-3.0 License

## Issue and Pull requests
Actually,I am not able to write codes.
So Sorry I can't solve your problems in many times.
If you PR,I'll merge it.

## Running Environment
CPython 3.13(With Only Bsdiff4 Module)


## Working Status
x imgdiff
- transfer.list(V4):erase/bsdiff/move/new/stash/free/zero
- ApplyPatch.py(For patching boot with boot.img.p)

## Usage
### the BIU(BlockImageUpdate)
#### Usage
`python3 BlockImageUpdate.py <block file> <transfer list> <new data stream> <patch data stream> [--continue-on-error]`
#### args:
- block device (or file) to modify in-place
- transfer list (blob)
- new data stream (filename within package.zip)
- patch stream (filename within package.zip, must be uncompressed)
- --continue-on-error: continue execution even if commands fail


### the AP(ApplyPatch)
`python3 ApplyPatch.py <file> <target> <tgt_sha1> <size> <init_sha1(1)> <patch(1)> [init_sha1(2)] [patch(2)] ... [bonus]`
-        <file> = source file from rom zip
-        <target> = target file (use "-" to patch source file)
-        <tgt_sha1> = target SHA1 Sum after patching
-        <size> = file size
-        <init_sha1> = file SHA1 sum
-        <patch> = patch file (.p) from OTA zip
-        <bonus> = bonus resource file



## How I wrote this
I goto the [deepwiki page of imgpatchtools repository](https://deepwiki.com/erfanoabdi/imgpatchtools) and ask the ai to write a python3 implemention of the ApplyPatch and BlockImageUpdate.
With 2 days and 2 windows I opened to asked it about 100 times to make it completely and realstic as it can(but I still haven't fixed the move command wrong and many potential bugs)
I also have tried the other model like Kimi,Doubao(AI Code Mode) and Tencent-Hunyuan.
But they haven't write so completely like this version I post.
I Just Want to get a Multi-Platform Applying OTA to block images scripts😭😭

## How to fix bugs
goto the [deepwiki page of imgpatchtools repository](https://deepwiki.com/erfanoabdi/imgpatchtools) and send all to 