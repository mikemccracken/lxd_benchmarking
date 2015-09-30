#!/usr/bin/python3

from argparse import ArgumentParser
import os
import sys
from subprocess import check_output, STDOUT, CalledProcessError
import sqlite3
import time

if "GOPATH" not in os.environ:
    print("Please set GOPATH.")
    sys.exit(1)
LXD_SCRIPTS = os.path.join(os.environ.get("GOPATH", ""), "src", "github.com",
                           "lxc", "lxd", "scripts")

db = None
dbc = None
run_id = 0


def get_free_mem():
    free = check_output("free -m | awk '/Mem:/ { print $4 }'", shell=True)
    return int(free.decode())


def get_load():
    with open('/proc/loadavg', 'r') as loadavgf:
        loadavg = loadavgf.readlines()[0].split()[0]
    return float(loadavg)


def get_disk_usage():
    avail = check_output('df -BM --output=avail,target | grep " \/$"',
                         shell=True)
    return int(avail.decode().split()[0][:-1])


def import_image(image):
    print("importing image '{}'".format(image))
    check_output("{scriptpath}/lxd-images import {img} "
                 " --alias {img}".format(scriptpath=LXD_SCRIPTS,
                                         img=image), shell=True)
    print("done importing image '{}'".format(image))


def delete_image(image):
    check_output("lxc image delete {img}".format(img=image), shell=True)


def setup_backend(backend, opts):
    if backend == "lvm":
        try:
            check_output("sudo {}/lxd-setup-lvm-storage "
                         "-s 10G".format(LXD_SCRIPTS),
                         shell=True, stderr=STDOUT, env=os.environ.copy())
        except CalledProcessError as e:
            print("output:" + e.output.decode())
            raise e
    import_image(opts.image)


def teardown_backend(backend, opts):
    delete_image(opts.image)
    if backend == "lvm":
        check_output("sudo -E {}/lxd-setup-lvm-storage "
                     "--destroy".format(LXD_SCRIPTS),
                     shell=True)


def do_launch(count, backend, opts):
    tgtfmt = "ctr-{i}-" + backend
    cmdfmt = "lxc launch " + opts.image + " {target}"
    return do('launch', [(cmdfmt, tgtfmt)], count, backend, opts)


def do_list(count, tag, backend, opts):
    cmdfmt = "lxc list"
    tgtfmt = ""
    return do('list-' + tag, [(cmdfmt, tgtfmt)], count, backend, opts)


def do_delete(to_delete, tag, backend, opts):
    cmds = ["lxc delete " + n for n in to_delete]
    do('delete-' + tag, cmds, 0, backend, opts)


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

    start_mem = get_free_mem()
    start_load = get_load()
    start_disk = get_disk_usage()
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
    mem_increase = get_free_mem() - start_mem
    load_increase = get_load() - start_load
    disk_increase = get_disk_usage() - start_disk

    record_batch(batchname, time_all, recs, count, backend,
                 mem_increase, load_increase, disk_increase, opts)
    return completed_tgts


def record_batch(name, time_all, recs, count, backend, mem_increase,
                 load_increase, disk_increase, opts):
    recavg = sum([t for _, t in recs]) / len(recs)
    print("{} n={}: tot={} avg={}".format(name, len(recs), time_all, recavg))

    dbc.execute("INSERT INTO timings VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (name, backend, len(recs), count, time_all, recavg,
                 mem_increase, load_increase, disk_increase,
                 opts.image, run_id))


def run_bench(opts):
    for count in opts.counts.split(','):
        count = int(count)
        for backend in opts.backends.split(','):
            print("# N={}, backend={}".format(count, backend))
            setup_backend(backend, opts)

            launched = do_launch(count, backend, opts)
            do_list(count, "containers", backend, opts)
            do_delete(launched, 'containers', backend, opts)

            src = do_launch(1, backend, opts)[0]
            copies = do_copy(src, count, backend, opts)
            do_list(count, "copies", backend, opts)
            do_delete(copies, 'copies', backend, opts)

            do_snapshot(src, count, backend, opts)
            # deleting src will delete the snapshots too:
            do_delete([src], 'container-with-snaps', backend, opts)
            teardown_backend(backend, opts)

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
    p.add_argument("-m", dest='message', default="",
                   help="message about run")
    opts = p.parse_args(sys.argv[1:])
    db = sqlite3.connect("bench.db")
    dbc = db.cursor()

    dbc.execute("CREATE TABLE if not exists runs "
                "(id integer primary key, argv text, date date, message text)")
    dbc.execute("CREATE TABLE if not exists timings "
                "(batch text, backend text, numrecs int, count int, "
                "total_time real, avg_time real, "
                "mem_increase int, load_increase real, disk_increase int, "
                "image text, run_id int)")
    dbc.execute("INSERT INTO runs(argv, date, message) "
                "VALUES(?, date('now'), ?)",
                (str(sys.argv[1:]), opts.message))
    dbc.execute("select max(id) + 1 from runs")
    run_id = dbc.fetchone()[0]

    try:
        run_bench(opts)
    except Exception as e:
        print("Stopped because of an error. Go clean me up, sorry")
        print(e)
    finally:
        db.commit()
        db.close()
    print("Done, OK")
