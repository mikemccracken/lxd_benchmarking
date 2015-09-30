#!/usr/bin/python3

from argparse import ArgumentParser
import sys
from subprocess import check_output, STDOUT, DEVNULL, CalledProcessError
import time


def setup_backend(backend, opts):
    print("TODO: set up backends")


def do_launch(count, backend, opts):
    tgtfmt = "ctr-{i}-" + backend
    cmdfmt = "lxc launch " + opts.image + " {target}"
    return do('launch', [(cmdfmt, tgtfmt)], count, backend, opts)


def do_delete(to_delete, backend, opts):
    cmds = ["lxc delete " + n for n in to_delete]
    do('delete', cmds, 0, backend, opts)


def do_copy(source, count, backend, opts):
    tgtfmt = "copy-{i}-backend"
    cmdfmt = "lxc copy " + source + " {target}"
    return do('copy', [(cmdfmt, tgtfmt)], count, backend, opts)


def do_snapshot(source, count, backend, opts):
    tgtfmt = "snap-{i}-" + backend
    cmdfmt = "lxc snapshot " + source + " {target}"
    snaps = do('snapshot', [(cmdfmt, tgtfmt)], count, backend, opts)
    return [source + "/" + snap for snap in snaps]


def do(batchname, cmdfmts, count, backend, opts):
    recs = []
    cmds = []
    completed_tgts = []
    if count > 0:
        if len(cmdfmts) != 1:
            print("Whoops, expected a single cmd fmt, got " + cmdfmts)
            sys.exit()
        for i in range(count):
            cmdfmt, tgtfmt = cmdfmts[0]
            tgt = tgtfmt.format(i=i)
            cmd = cmdfmt.format(target=tgt, backend=backend)
            cmds.append((cmd, tgt))
    else:
        cmds = [(c, "") for c in cmdfmts]

    start_all = time.time()
    for cmd, tgt in cmds:
        start = time.time()
        if opts.verbose:
            print("+ " + cmd)
        try:
            check_output(cmd, shell=True, stderr=STDOUT)
            completed_tgts.append(tgt)
        except CalledProcessError as e:
            print("error: {}".format(e))
            print("output: " + e.output.decode())
            raise Exception("Fatal ERROR")

        if opts.verbose:
            print("=> OK")

        recs.append((cmd, time.time() - start))
    time_all = time.time() - start_all
    record_batch(batchname, time_all, recs, opts)
    return completed_tgts


def record_batch(name, time_all, recs, opts):
    print("Batch {}: {}".format(name, time_all))
    print(recs)                 # TODO better


def run_bench(opts):
    for count in opts.counts.split(','):
        count = int(count)
        for backend in opts.backends.split(','):
            setup_backend(backend, opts)

            launched = do_launch(count, backend, opts)
            do_delete(launched, backend, opts)

            launched = do_launch(1, backend, opts)[0]
            copies = do_copy(launched, count, backend, opts)
            do_delete(copies, backend, opts)

            snapshots = do_snapshot(launched, count, backend, opts)
            do_delete(snapshots, backend, opts)

if __name__ == "__main__":
    p = ArgumentParser(description="LXD storage bencher")
    p.add_argument("counts",
                   help="comma separated list of counts of"
                   " containers/snapshots/copies to bench")
    p.add_argument("backends",
                   help="a comma separated list of backends to use.",
                   default="lvm,zfs,dir,btrfs")
    p.add_argument("--image", default='ubuntu',
                   help="Image hash or alias to use")
    p.add_argument("-v", "--verbose", action='store_true')
    opts = p.parse_args(sys.argv[1:])
    try:
        run_bench(opts)
    except:
        print("Stopped because of an error. Go clean me up, sorry")
    print("Done, OK")
