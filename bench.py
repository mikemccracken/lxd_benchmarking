#!/usr/bin/python3

from argparse import ArgumentParser
import os
import shutil
import signal
import sys
from subprocess import (check_output, STDOUT, CalledProcessError,
                        Popen, call)
import sqlite3
import tabulate
from tempfile import mkdtemp
import time
import traceback

if "GOPATH" not in os.environ:
    print("Please set GOPATH.")
    sys.exit(1)
LXD_SRC_DIR = os.path.join(os.environ.get("GOPATH", ""), "src", "github.com",
                           "lxc", "lxd")
LXD_SCRIPTS_DIR = os.path.join(LXD_SRC_DIR, "scripts")

db = None
dbc = None
run_id = 0

sigusr_received = False


def handle_sigusr1(signum, frame):
    global sigusr_received
    print("got stop signal.")
    sigusr_received = True


def get_free_mem(include_cached=False):
    fmt = "$4"
    if include_cached:
        fmt += "+$7"
    free = check_output("free -m | awk '/Mem:/ { print " + fmt + "  }'",
                        shell=True)
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
    if os.path.exists(image):
        print("importing image from file {}".format(image))
        call("lxc image import meta-{0} {0} --alias img".format(image),
             shell=True)
    else:
        print("importing image '{}'".format(image))
        try:
            check_output("{scriptpath}/lxd-images import {image} "
                         " --alias img".format(scriptpath=LXD_SCRIPTS_DIR,
                                               image=image),
                         shell=True, stderr=STDOUT)
        except CalledProcessError as e:
            print("out, \n{}".format(e.output))
            raise e
    print("done importing image '{}'".format(image))


def delete_image():
    check_output("lxc image delete img", shell=True)


def setup_backend(backend, tmp_dir, opts):
    lxd_dir = os.path.join(tmp_dir, 'lxd_dir')
    os.makedirs(lxd_dir, exist_ok=True)

    if backend == "dir":
        lxd_proc = spawn_lxd(tmp_dir)
        return dict(lxd_proc=lxd_proc)

    if backend == "lvm":
        lxd_proc = spawn_lxd(tmp_dir)
        if opts.blockdev == "loop":
            try:
                check_output("sudo -E {}/lxd-setup-lvm-storage "
                             "-s 10G".format(LXD_SCRIPTS_DIR),
                             shell=True, stderr=STDOUT, env=os.environ.copy())
            except CalledProcessError as e:
                print("output: " + e.output.decode())
                raise e
        else:
            check_output("sudo pvcreate {}".format(opts.blockdev), shell=True)
            check_output("sudo vgcreate LXDStorage {}".format(opts.blockdev),
                         shell=True)
            check_output("lxc config set storage.lvm_vg_name LXDStorage",
                         shell=True)

        return dict(lxd_proc=lxd_proc)

    elif backend in ['btrfs', 'zfs']:
        backingfile = None
        if opts.blockdev == "loop":
            backingfile = backend + '.img'
            check_output("truncate -s {} {}".format('10G', backingfile),
                         shell=True)
            dev = check_output("sudo losetup -f",
                               shell=True).decode().strip()
            check_output("sudo losetup {} {}".format(dev, backingfile),
                         shell=True)
        else:
            dev = opts.blockdev

        if backend == 'btrfs':
            check_output("sudo mkfs.btrfs -m single {}".format(dev),
                         shell=True)
            check_output("sudo mount {} {}".format(dev, lxd_dir),
                         shell=True)
            lxd_proc = spawn_lxd(tmp_dir)
        else:
            check_output("sudo zpool create -m none "
                         "LXDStoragePool {}".format(dev),
                         shell=True)
            lxd_proc = spawn_lxd(tmp_dir)
            check_output("lxc config set storage.zfs_pool_name LXDStoragePool",
                         shell=True)

        return dict(lxd_proc=lxd_proc, lxd_dir=lxd_dir,
                    dev=dev, backingfile=backingfile)
    else:
        raise Exception("Unknown backend " + backend)


def teardown_backend(backend, tmp_dir, info, opts):
    if backend == "dir":
        teardown_lxd(tmp_dir, info['lxd_proc'], opts)

    elif backend == "lvm":
        if opts.blockdev == 'loop':
            check_output("sudo -E {}/lxd-setup-lvm-storage "
                         "--destroy".format(LXD_SCRIPTS_DIR),
                         shell=True)
        else:
            check_output("sudo vgremove -f LXDStorage", shell=True)
            check_output("sudo pvremove -f {}".format(opts.blockdev),
                         shell=True)

        teardown_lxd(tmp_dir, info['lxd_proc'], opts)

    elif backend in ['btrfs', 'zfs']:
        if backend == 'btrfs':
            teardown_lxd(tmp_dir, info['lxd_proc'], opts)
            check_output("sudo umount {}".format(info['lxd_dir']), shell=True)
        else:
            check_output("lxc config unset storage.zfs_pool_name", shell=True)
            check_output("sudo zpool destroy LXDStoragePool", shell=True)
            teardown_lxd(tmp_dir, info['lxd_proc'], opts)

        if opts.blockdev == 'loop':
            check_output("sudo losetup -d {}".format(info['dev']), shell=True)
            check_output("sudo rm -f {}".format(info['backingfile']),
                         shell=True)
        else:
            check_output("sudo wipefs -a {}".format(info['dev']), shell=True)


def do_launch(count, backend, opts, record=True):
    tgtfmt = "ctr-{i}-" + backend
    cmdfmt = "lxc launch img {target}"
    return do_fmt('launch', cmdfmt, tgtfmt, count, backend, opts,
                  record=record)


def do_list(count, tag, backend, opts):
    cmds = ["lxc list"]
    return do_cmds('list-' + tag, cmds, count, backend, opts)


def do_delete(to_delete, tag, count, backend, opts):
    cmds = ["lxc delete  " + n for n in to_delete]
    do_cmds('delete-' + tag, cmds, count, backend, opts)


def wait_for_cloudinit_done(container):
    print("waiting for " + container)
    for i in range(20):
        time.sleep(2)
        try:
            result_json = check_output("lxc exec -- "
                                       "sudo cat /run/cloud-init/result.json",
                                       stderr=STDOUT,
                                       shell=True)
        except:
            continue

        if result == '':
            continue

        try:
            ret = json.loads(result_json)
        except:
            continue

        errors = ret['v1']['errors']
        if len(errors):
            print(errors)
            raise Exception("Cloud-init threw an error", errors)
        break


def do_pause(to_pause, count, backend, opts):
    cmds = ["lxc pause " + name for name in to_pause]
    do_cmds('pause', cmds, count, backend, opts, record=False)


def do_copy(source, count, backend, opts):
    tgtfmt = "copy-{i}-" + backend
    cmdfmt = "lxc copy  " + source + " {target}"
    return do_fmt('copy', cmdfmt, tgtfmt, count, backend, opts)


def do_snapshot(source, count, backend, opts):
    tgtfmt = "snap-{i}-" + backend
    cmdfmt = "lxc snapshot  " + source + " {target}"
    snaps = do_fmt('snapshot', cmdfmt, tgtfmt, count, backend, opts)
    return [source + "/" + snap for snap in snaps]


# do a formatted command 'nexec' times.
def do_fmt(batchname, cmdfmt, tgtfmt, count, backend, opts,
           nexec=None, record=True):
    cmds = []
    tgts = []
    if nexec is None:
        nexec = count
    for i in range(nexec):
        tgt = tgtfmt.format(i=i)
        cmd = cmdfmt.format(target=tgt, backend=backend)
        cmds.append(cmd)
        tgts.append(tgt)

    completed = do_cmds(batchname, cmds, count, backend, opts,
                        targets=tgts, record=record)
    return completed


# do a list of things, ignoring but recording N
def do_cmds(batchname, cmds, count, backend, opts, targets=None,
            record=True):
    if sigusr_received:
        print("skipping.")
        return []

    def log(s):
        if opts.verbose:
            print(s)

    recs = []
    completed_tgts = []
    if targets is None:
        targets = cmds

    assert(len(cmds) == len(targets))

    start_mem = get_free_mem()
    start_load = get_load()
    start_disk = get_disk_usage()
    start_all = time.time()
    last_stoptime = None
    for cmd, tgt in zip(cmds, targets):
        start = time.time()
        log("+ " + str(cmd))
        try:
            check_output(cmd, shell=True, stderr=STDOUT)
            completed_tgts.append(tgt)
        except CalledProcessError as e:
            print("error: {}".format(e))
            print("output: " + e.output.decode())
            raise Exception("Fatal ERROR")
        last_stoptime = time.time()
        dur = last_stoptime - start
        log("=> OK, {:2f} sec".format(dur))

        recs.append((cmd, dur))

        if get_free_mem(include_cached=True) <= opts.mem_threshold:
            print("stopping after {}, ran out of memory".format(len(recs)))
            break
        if dur > opts.duration_threshold:
            print("stopping after {}, got impatient".format(len(recs)))
            break
        if sigusr_received:
            print("stopping after {}, user asked to halt.".format(len(recs)))
            break
    time_all = last_stoptime - start_all
    mem_increase = get_free_mem() - start_mem
    load_increase = get_load() - start_load
    disk_increase = get_disk_usage() - start_disk

    if record:
        record_batch(batchname, time_all, recs, count, backend,
                     mem_increase, load_increase, disk_increase, opts)
    return completed_tgts


def record_batch(name, time_all, recs, count, backend, mem_increase,
                 load_increase, disk_increase, opts):
    recavg = sum([t for _, t in recs]) / len(recs)

    c = dbc.execute("INSERT INTO timings(batch, backend, numrecs, count, "
                    "total_time, avg_time, mem_increase, load_increase, "
                    "disk_increase, image, run_id ) VALUES "
                    "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (name, backend, len(recs), count, time_all, recavg,
                     mem_increase, load_increase, disk_increase,
                     opts.image, run_id))
    timings_id = c.lastrowid
    for cmd, dur in recs:
        dbc.execute("INSERT INTO recs(cmd, duration, timings_id) VALUES "
                    "(?, ?, ?)", (cmd, dur, timings_id))

    db.commit()


def spawn_lxd(temp_dir):
    lxd_config = os.path.join(temp_dir, 'lxd_config')
    os.makedirs(lxd_config)
    os.environ["LXD_CONF"] = lxd_config
    lxd_dir = os.path.join(temp_dir, "lxd_dir")
    os.environ["LXD_DIR"] = lxd_dir
    for fn in ['server.crt', 'server.key']:
        shutil.copyfile(os.path.join(LXD_SRC_DIR, 'test', 'deps', fn),
                        os.path.join(temp_dir, fn))
    lxd_proc = Popen(["sudo", "-E", "{}/bin/lxd".format(os.environ["GOPATH"]),
                      "--group", "lxd", "--logfile",
                      "{}/lxd.log".format(lxd_dir)])

    print("waiting for spawned lxd")
    rv = -1
    while rv != 0:
        cmd = "lxc finger "
        rv = call(cmd, shell=True)
        time.sleep(1.5)

    # check_call("lxc config  "
    #            " set core.https_address 127.0.0.1:22222",
    #            shell=True)
    # check_call("lxc config  "
    #            "set core.trust_password foo", shell=True)
    return lxd_proc


def teardown_lxd(tmp_dir, lxd_proc, opts):
    lxd_pid = check_output("pgrep -P " + str(lxd_proc.pid),
                           shell=True)
    lxd_pid = int(lxd_pid.decode())
    print("killing lxd pid {}".format(lxd_pid))

    call("sudo kill -15 {}".format(lxd_pid), shell=True)
    time.sleep(2)
    call("sudo kill -9 {}".format(lxd_pid), shell=True)

    while True:
        try:
            pid = check_output("ps aux | grep lxc-monitord "
                               "| grep {}/lxd_dir "
                               "| grep -v grep".format(tmp_dir),
                               shell=True)
            pid = pid.decode().split()[1]
            call("sudo kill -9 {}".format(pid), shell=True)
        except CalledProcessError:
            break
        time.sleep(1)


def run_bench(opts):
    for backend in opts.backends.split(','):
        print("* backend = {}".format(backend))
        tmp_dir = mkdtemp(prefix="lxd_tmp_dir")
        call("chmod +x {}".format(tmp_dir), shell=True)
        binfo = setup_backend(backend, tmp_dir, opts)
        print("** check backend is set up:")
        call("lxc finger --debug", shell=True)
        import_image(opts.image)
        try:
            for count in opts.counts.split(','):
                count = int(count)
                print("** N = {}".format(count))

                print("*** launching {} containers".format(count))
                launched = do_launch(count, backend, opts)
                print("*** listing")
                do_list(count, "containers", backend, opts)
                print("*** deleting")
                do_delete(launched, 'containers', count, backend, opts)

                launched = do_launch(1, backend, opts, record=False)
                if len(launched) > 0:
                    src = launched[0]
                    if 'ubuntu' in opts.image:
                        wait_for_cloudinit_done(src)
                    print("*** pausing " + src)
                    do_pause([src], count, backend, opts)
                    print("*** making {} copies".format(count))
                    copies = do_copy(src, count, backend, opts)
                    do_list(count, "copies", backend, opts)
                    print("*** deleting the copies")
                    do_delete(copies, 'copies', count, backend, opts)

                    print("*** making {} snapshots".format(count))
                    do_snapshot(src, count, backend, opts)
                    print("*** cleaning up {} and snaps".format(src))
                    # deleting src will delete the snapshots too:
                    do_delete([src], 'container-with-snaps', count, backend,
                              opts)
                print("*** check that we're clean:")
                call("lxc list", shell=True)
        except:
            print("Stopped because of an error. Go clean me up, sorry.")
            print(traceback.format_exc())
            print("try LXD_DIR={}/lxd_dir lxc list".format(tmp_dir))
            input("done poking? ")

        finally:
            delete_image()
            teardown_backend(backend, tmp_dir, binfo, opts)
            if not opts.keep:
                call("sudo rm -rf {}".format(tmp_dir), shell=True)


def show_report(the_id, csv=False, showall=False):
    dbc.execute("SELECT * FROM runs where id = ?", (the_id, ))
    run_rows = dbc.fetchall()
    print(tabulate.tabulate(run_rows))
    dbc.execute("SELECT * FROM timings WHERE run_id = {} "
                "ORDER BY batch, count, numrecs".format(the_id))
    rows = dbc.fetchall()
    headers = ['id', 'batch', 'backend', 'numrecs', 'count',
               'total_time', 'avg_time', 'mem_inc', 'load_inc', 'disk_inc',
               'image', 'runid']
    if csv:
        fmt = tabulate.simple_separated_format(",")
    else:
        fmt = 'simple'

    if not showall:
        print(tabulate.tabulate(rows, headers=headers, tablefmt=fmt,
                                floatfmt='.3g'))
        return

    for row in rows:
        print("\n\n")
        print(tabulate.tabulate([row], headers=headers, tablefmt=fmt,
                                floatfmt='.3g'))
        id = row[0]
        dbc.execute("SELECT * FROM recs WHERE timings_id = ?", (id,))
        print("\n")
        print(tabulate.tabulate(dbc.fetchall(), headers=['cmd', 'dur', 'id'],
                                floatfmt='.3g'))


def show_runs():
    dbc.execute("SELECT id, date, message FROM runs")
    rows = dbc.fetchall()
    print(tabulate.tabulate(rows))


def init_db():
    global db, dbc
    db = sqlite3.connect("bench.db")
    dbc = db.cursor()

    dbc.execute("CREATE TABLE if not exists runs "
                "(id integer primary key, argv text, date date, message text)")
    dbc.execute("CREATE TABLE if not exists timings "
                "(id integer primary key, "
                "batch text, backend text, numrecs int, count int, "
                "total_time real, avg_time real, "
                "mem_increase int, load_increase real, disk_increase int, "
                "image text, run_id int)")
    dbc.execute("CREATE TABLE if not exists recs "
                "(id integer primary key, cmd text, duration real, "
                " timings_id int) ")


if __name__ == "__main__":
    p = ArgumentParser(description="LXD storage bencher")
    p.add_argument("-v", "--verbose", action='store_true')

    sps = p.add_subparsers(dest="subcommand_name",
                           help='sub-command help???')

    run_p = sps.add_parser('run', help='run a bench help')
    run_p.add_argument("counts",
                       help="comma separated list of counts of"
                       " containers/snapshots/copies to bench")
    run_p.add_argument("backends",
                       help="a comma separated list of backends to use.",
                       default="lvm,zfs,dir,btrfs")
    run_p.add_argument("--image", default='ubuntu',
                       help="image to use - one of 'ubuntu', 'busybox', or a "
                       "filename of an exported image tarball")
    run_p.add_argument("-m", dest='message', default="",
                       help="message about run")
    run_p.add_argument("--keep", default=False,
                       help="do not tear down lxd dirs")
    run_p.add_argument("--blockdev", default='loop',
                       help="block device to use for storage backends")
    run_p.add_argument("--mem-threshold", dest="mem_threshold",
                       default=512, type=int,
                       help="Stop a trial before we have less than this much "
                       "free + cached RAM in MB")
    run_p.add_argument("--runtime-threshold", dest="duration_threshold",
                       default=600, type=int,
                       help="Stop a trial after this many seconds")

    show_p = sps.add_parser('show', help='show runs')
    show_p.add_argument("--run", dest="run_id", help="id to show",
                        default=None)
    show_p.add_argument("--csv", action='store_true',
                        help="Show results as csv")
    show_p.add_argument("-a", action='store_true',
                        dest='showall', help="show all recs")
    opts = p.parse_args(sys.argv[1:])

    init_db()

    if opts.subcommand_name == 'run':

        signal.signal(signal.SIGUSR1, handle_sigusr1)
        print("Running. Use 'kill -USR1 {}' to stop a "
              "batch".format(os.getpid()))
        dbc.execute("INSERT INTO runs(argv, date, message) "
                    "VALUES(?, datetime('now'), ?)",
                    (str(sys.argv[1:]), opts.message))
        dbc.execute("select max(id) from runs")
        run_id = dbc.fetchone()[0]

        try:
            run_bench(opts)
            show_report(run_id)
        finally:
            db.commit()
            db.close()

    elif opts.subcommand_name == 'show':
        if opts.run_id is None:
            show_runs()
        else:
            show_report(opts.run_id, csv=opts.csv, showall=opts.showall)

    print("Done, OK")
