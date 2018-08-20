import base.Util as util

def log(content):
    string = "[Time: " + util.getYMDHMS() + "]: " + str(content)
    print(string)
    return string