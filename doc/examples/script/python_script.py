import sys, json
ARGS = json.load(open(sys.argv[1]))
PARAMS = ARGS.pop("params")
IN = ARGS.pop("in")
OUT = ARGS.pop("out")


with open(IN["greeting"]) as rh, open(OUT["package"], "wb") as wh:
    wh.write("bash says: %s" % rh.read())
    wh.write("python says: %s" % "happy!")

