# -*- coding: utf-8 -*-
""" 
:mod:`papy.util.script`
=======================

Provides ``script`` a ``Worker`` function to interect with arbitrary 
scripts.

"""
import os
import json
from time import sleep
from glob import glob
from subprocess import Popen, PIPE, CalledProcessError
from tempfile import NamedTemporaryFile

CMD = \
"""
%s
run() {
  (eval %s) && echo 0 >> "%s" || echo 1 >> "%s"
}
run 
exit 0
"""

PY_TEMPLATE = """import sys, json
ARGS = json.load(open(sys.argv[1]))
PARAMS = ARGS.pop("params")
IN = ARGS.pop("in")
OUT = ARGS.pop("out")
"""

SH_TEMPLATE = """eval $(python2 - <<EOF
import sys, json, itertools

ARGS = json.load(open("$1"))
PARAMS = ARGS.pop("params")
IN = ARGS.pop("in")
OUT = ARGS.pop("out")

T_PARAMS = "declare -A PARAMS=( " + " ".join(['[%s]="%s"'] * len(PARAMS)) + " )"
F_PARAMS =  T_PARAMS % tuple(itertools.chain(*zip(PARAMS.keys(), PARAMS.values())))
T_IN = "declare -A IN=( " + " ".join(['[%s]="%s"'] * len(IN)) + " )"
F_IN = T_IN % tuple(itertools.chain(*zip(IN.keys(), IN.values())))
T_OUT = "declare -A OUT=( " + " ".join(['[%s]="%s"'] * len(OUT)) + " )"
F_OUT = T_OUT % tuple(itertools.chain(*zip(OUT.keys(), OUT.values())))

print "; ".join((F_PARAMS, F_IN, F_OUT))
EOF
)
# here script
exit 0
"""

def _eval_cmd(evaluator, preamble, dir, cmd):
    with NamedTemporaryFile(dir=dir) as wrapper, \
         NamedTemporaryFile(dir=dir) as status:
        wrapper.file.write(CMD % (preamble, cmd, status.name, status.name))
        wrapper.file.flush()
        try:
            proc = Popen([evaluator, wrapper.name], stdin=None, stdout=PIPE, 
                         stderr=PIPE)
        except OSError:
            return (1, "", "subprocess call failed: %s" % evaluator)
        stdout, stderr = proc.communicate(None)
        err = status.file.read()
        while len(err) == 0:
            err = status.file.read()
            sleep(0.1)
    code = int(err)
    return (code, stdout, stderr)

def _eval_script(evaluator, preamble, dir, executable, script, args):
    if glob(dir + "/*"):
        raise Exception("directory: %s not empty" % dir) 
    if not os.path.exists(dir):
        os.mkdir(dir)
    with NamedTemporaryFile(dir=dir) as args_:
        args_.file.write(json.dumps(args))
        args_.file.flush()
        cmd = "%s %s %s" % (executable, script, args_.name)
        ret = _eval_cmd(evaluator, preamble, dir, cmd)
    return ret


def write_template(fn, lang="python"):
    """
    Write language-specific script template to file.

    Arguments:

      - fn(``string``) path to save the template to
      - lang('python', 'bash') which programming language
      
    """
    with open(fn, "wb") as fh:
        if lang == "python":
            fh.write(PY_TEMPLATE)
        elif lang == "bash":
            fh.write(SH_TEMPLATE)

def get_config(lang="python"):
    """
    Returns language-specific script configuration.

    Arguments:

      - lang('python', 'bash') which programming language

    """
    configs = {
        "bash" : {
            "evaluator":"bash",
            "preamble":"",
            "dir":"{{os.getcwd()}}",
            "executable":"bash",
            "script":"{{script_name.sh}}",
            "1to1":False,
            "in":(
                "{{port_name}}",
            ),
            "out":(
                ("{{port_name}}", "{{file_extension}}"), 
            ),
            "params":{}
        },
        "python": {
            "evaluator":"bash",
            "preamble":"",
            "dir":"{{os.getcwd()}}",
            "executable":"python2",
            "script":"{{script_name.py}}",
            "multi":False,
            "in":(
                "{{port_name}}", 
            ),
            "out":(
                ("{{port_name}}", "{{file_extension}}"),
            ),
            "params":{}
        }}
    return configs[lang]

def script(inbox, cfg):
    """
    Execute arbitrary scripts. 

    Arguments:
    
      - cfg(``dict``) script configuartion dictionary

    """
    args = {}
    args["params"] = dict(cfg["params"])
    args["in"] = {}
    for in_port in cfg["in"]:
        for inin_ports in inbox:
            in_path = inin_ports.get(in_port, None)
            if (in_path is not None):
                # first matching input-output (including type) port is linked remaining ignored
                args["in"][in_port] = in_path
                break
    # check that all input ports are connected
    if len(args["in"]) < len(cfg["in"]):
        raise Exception("not all in_ports connected, got: %s" (args["in"],))
    # create output file for out_ports
    args["out"] = {}
    out = {}
    for i, (out_port, out_ext) in enumerate(cfg["out"]):
        if cfg["in"] == tuple(out_port_ for out_port_, _ in cfg["out"]):
            pfx = args["in"][cfg["in"][i]].split("/")[-1].split(".")[0] + "_"
            base = cfg["id"]
        else:
            pfx = args["in"][cfg["in"][0]].split("/")[-1].split(".")[0] + "_"
            base = cfg["id"] + "#" + out_port 
        if out_ext:
            out_path = cfg["dir"] + "/" + pfx + base + "." + out_ext
        else:
            out_path = cfg["dir"] + "/" + pfx + base
        args["out"][out_port] = out_path
        out[out_port] = out_path
    # evaluate and check for errors
    ret = _eval_script(cfg["evaluator"], cfg["preamble"], cfg["dir"], cfg["executable"], cfg["script"], args)
    if ret[0] != 0:
        raise Exception(ret[0], cfg["script"], ret[1], ret[2])
    return out

