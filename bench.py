#!/usr/bin/python3

from argparse import ArgumentParser
import os
import shutil
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
    try:
        check_output("{scriptpath}/lxd-images import {img} "
                     " --alias {img}".format(scriptpath=LXD_SCRIPTS_DIR,
                                             img=image), shell=True,
                     stderr=STDOUT)
    except CalledProcessError as e:
        print("out, \n{}".format(e.output))
        raise e
    print("done importing image '{}'".format(image))


def delete_image(image):
    check_output("lxc image  "
                 "delete {img}".format(img=image), shell=True)


def setup_backend(backend, tmp_dir, opts):
    lxd_dir = os.path.join(tmp_dir, 'lxd_dir')
    os.makedirs(lxd_dir, exist_ok=True)

    if backend == "dir":
        lxd_proc = spawn_lxd(tmp_dir)
        return dict(lxd_proc=lxd_proc)

    if backend == "lvm":
        lxd_proc = spawn_lxd(tmp_dir)
        try:
            check_output("sudo -E {}/lxd-setup-lvm-storage "
                         "-s 10G".format(LXD_SCRIPTS_DIR),
                         shell=True, stderr=STDOUT, env=os.environ.copy())
        except CalledProcessError as e:
            print("output: " + e.output.decode())
            raise e
        return dict(lxd_proc=lxd_proc)

    elif backend == 'btrfs':
        backingfile = None
        if opts.blockdev == "loop":
            backingfile = 'btrfs.img'
            check_output("truncate -s {} {}".format('10G', backingfile),
                         shell=True)
            dev = check_output("sudo losetup -f",
                               shell=True).decode().strip()
            check_output("sudo losetup {} btrfs.img".format(dev),
                         shell=True)
        else:
            dev = opts.blockdev
        check_output("sudo mkfs.btrfs -m single {}".format(dev),
                     shell=True)
        check_output("sudo mount {} {}".format(dev, lxd_dir),
                     shell=True)
        lxd_proc = spawn_lxd(tmp_dir)
        return dict(lxd_proc=lxd_proc, lxd_dir=lxd_dir,
                    dev=dev, backingfile=backingfile)

    elif backend == 'zfs':
        pass
    else:
        raise Exception("Unknown backend " + backend)


def teardown_backend(backend, tmp_dir, info, opts):
    if backend == "dir":
        teardown_lxd(tmp_dir, info['lxd_proc'], opts)
    elif backend == "lvm":
        check_output("sudo -E {}/lxd-setup-lvm-storage "
                     "--destroy".format(LXD_SCRIPTS_DIR),
                     shell=True)
        teardown_lxd(tmp_dir, info['lxd_proc'], opts)
    elif backend == 'btrfs':
        teardown_lxd(tmp_dir, info['lxd_proc'], opts)
        check_output("sudo umount {}".format(info['lxd_dir']), shell=True)
        if opts.blockdev == 'loop':
            check_output("sudo losetup -d {}".format(info['dev']), shell=True)
            check_output("sudo rm -f {}".format(info['backingfile']),
                         shell=True)


def do_launch(count, backend, opts):
    tgtfmt = "ctr-{i}-" + backend
    cmdfmt = "lxc launch  " + opts.image + " {target}"
    return do('launch', [(cmdfmt, tgtfmt)], count, backend, opts)


def do_list(count, tag, backend, opts):
    cmdfmt = "lxc list "
    tgtfmt = ""
    return do('list-' + tag, [(cmdfmt, tgtfmt)], count, backend, opts)


def do_delete(to_delete, tag, backend, opts):
    cmds = ["lxc delete  " + n for n in to_delete]
    do('delete-' + tag, cmds, 0, backend, opts)


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


def do_pause(to_pause, backend, opts):
    cmds = ["lxc pause " + to_pause]
    do('pause', cmds, 0, backend, opts)


def do_copy(source, count, backend, opts):
    tgtfmt = "copy-{i}-" + backend
    cmdfmt = "lxc copy  " + source + " {target}"
    return do('copy', [(cmdfmt, tgtfmt)], count, backend, opts)


def do_snapshot(source, count, backend, opts):
    tgtfmt = "snap-{i}-" + backend
    cmdfmt = "lxc snapshot  " + source + " {target}"
    snaps = do('snapshot', [(cmdfmt, tgtfmt)], count, backend, opts)
    return [source + "/" + snap for snap in snaps]


def do(batchname, cmdfmts, count, backend, opts):

    def log(s):
        if opts.verbose:
            print(s)

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
        log("+ " + cmd)
        try:
            check_output(cmd, shell=True, stderr=STDOUT)
            completed_tgts.append(tgt)
        except CalledProcessError as e:
            print("error: {}".format(e))
            print("output: " + e.output.decode())
            raise Exception("Fatal ERROR")

        log("=> OK")

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

    dbc.execute("INSERT INTO timings VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (name, backend, len(recs), count, time_all, recavg,
                 mem_increase, load_increase, disk_increase,
                 opts.image, run_id))


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
    call("lxc finger --debug", shell=True)
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
        print("# backend = {}".format(backend))
        tmp_dir = mkdtemp(prefix="lxd_tmp_dir")
        call("chmod +x {}".format(tmp_dir), shell=True)
        binfo = setup_backend(backend, tmp_dir, opts)
        import_image(opts.image)
        try:
            for count in opts.counts.split(','):
                count = int(count)
                print("## N = {}".format(count))

                print("launching {} containers".format(count))
                launched = do_launch(count, backend, opts)
                print("listing")
                do_list(count, "containers", backend, opts)
                print("deleting")
                do_delete(launched, 'containers', backend, opts)

                src = do_launch(1, backend, opts)[0]
                if opts.image == 'ubuntu':
                    wait_for_cloudinit_done(src)
                do_pause(src, backend, opts)
                copies = do_copy(src, count, backend, opts)
                do_list(count, "copies", backend, opts)
                do_delete(copies, 'copies', backend, opts)

                do_snapshot(src, count, backend, opts)
                # deleting src will delete the snapshots too:
                do_delete([src], 'container-with-snaps', backend, opts)
        finally:
            delete_image(opts.image)
            teardown_backend(backend, tmp_dir, binfo, opts)
            if not opts.keep:
                call("sudo rm -rf {}".format(tmp_dir), shell=True)


def show_report(the_id, csv=False):
    dbc.execute("SELECT * FROM runs where id = ?", (the_id, ))
    run_rows = dbc.fetchall()
    print(tabulate.tabulate(run_rows))
    dbc.execute("SELECT * FROM timings WHERE run_id = {} "
                "ORDER BY count, batch".format(the_id))
    rows = dbc.fetchall()
    headers = ['batch', 'backend', 'numrecs', 'count',
               'total_time', 'avg_time', 'mem_inc', 'load_inc', 'disk_inc',
               'image', 'runid']
    if csv:
        fmt = tabulate.simple_separated_format(",")
    else:
        fmt = 'simple'
    print(tabulate.tabulate(rows, headers=headers, tablefmt=fmt,
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
                "(batch text, backend text, numrecs int, count int, "
                "total_time real, avg_time real, "
                "mem_increase int, load_increase real, disk_increase int, "
                "image text, run_id int)")


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
                       help="Image hash or alias to use")
    run_p.add_argument("-m", dest='message', default="",
                       help="message about run")
    run_p.add_argument("--keep", default=False,
                       help="do not tear down lxd dirs")
    run_p.add_argument("--blockdev", default='loop',
                       help="block device to use for storage backends")
    show_p = sps.add_parser('show', help='show runs')
    show_p.add_argument("--run", dest="run_id", help="id to show",
                        default=None)
    show_p.add_argument("--csv", action='store_true',
                        help="Show results as csv")
    opts = p.parse_args(sys.argv[1:])

    init_db()

    if opts.subcommand_name == 'run':

        dbc.execute("INSERT INTO runs(argv, date, message) "
                    "VALUES(?, datetime('now'), ?)",
                    (str(sys.argv[1:]), opts.message))
        dbc.execute("select max(id) from runs")
        run_id = dbc.fetchone()[0]

        try:
            run_bench(opts)
            show_report(run_id)
        except Exception as e:
            print("Stopped because of an error. Go clean me up, sorry")
            print(traceback.format_exc())
        finally:
            db.commit()
            db.close()

    elif opts.subcommand_name == 'show':
        if opts.run_id is None:
            show_runs()
        else:
            show_report(opts.run_id, opts.csv)

    print("Done, OK")
