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
        proc = Popen([evaluator, wrapper.name], stdin=None, stdout=PIPE, 
                     stderr=PIPE)
        stdout, stderr = proc.communicate(None)
        err = status.file.read()
        while len(err) == 0:
            err = status.file.read()
            sleep(0.1)
    code = int(err)
    return (code, stdout, stderr)

def _eval_script(evaluator, preamble, dir, executable, script, args):
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
            "in":(
                ("{{port_name}}", "file"),
            ),
            "out":(
                ("{{port_name}}", "file", "{{True if keep}}"), 
            ),
            "params":{}
        },
        "python": {
            "evaluator":"bash",
            "preamble":"",
            "dir":"{{os.getcwd()}}",
            "executable":"python",
            "script":"{{script_name.py}}",
            "in":(
                ("{{port_name}}", "file"), # input type file
            ),
            "out":(
                ("{{port_name}}", "file", "{{True if keep}}"),
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
    # copy params to args
    args = {}
    args["params"] = dict(cfg["params"])
    # connect child in_ports to parent out_ports
    args["in"] = {}
    for in_port, in_port_type in cfg["in"]:
        for out_ports in inbox:
            in_val, in_type, in_keep = out_ports.get(in_port, (None, None, None))
            if (in_val is not None) and (in_port_type == in_type):
                # first matching input-output (including type) port is linked remaining ignored
                args["in"][in_port] = in_val
                break
    # check that all input ports are connected
    if len(args["in"]) < len(cfg["in"]):
        raise Exception("not all in_ports connected, got: %s" (args["in"],))
    # create output file for out_ports
    args["out"] = {}
    out = {}
    for out_port, out_type, out_keep in cfg["out"]:
        pfx = args["in"][cfg["in"][0][0]].split("/")[-1].split(".")[0] + "_"
        sfx = "." + out_port
        out_val = NamedTemporaryFile(dir=cfg["dir"], prefix=pfx, suffix=sfx, delete=False).name
        args["out"][out_port] = out_val
        out[out_port] = (out_val, out_type, out_keep)
    # evaluate and check for errors
    ret = _eval_script(cfg["evaluator"], cfg["preamble"], cfg["dir"], cfg["executable"], cfg["script"], args)
    if ret[0] != 0:
        raise Exception(ret[0], cfg["script"], ret[1], ret[2])
    # remove files attached to input ports
    for in_port, in_port_type in cfg["in"]:
        if in_port_type == "file":
            for out_ports in inbox:
                in_val, in_type, in_keep = out_ports.get(in_port, (None, None, None))
                if (in_val is not None) and (in_port_type == in_type) and not in_keep:
                    os.unlink(in_val)
                    # only first matching was connected so only first matching is removed
                    break
    return out
