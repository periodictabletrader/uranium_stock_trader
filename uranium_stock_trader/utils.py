
def wrap_list(val):
    if isinstance(val, list):
        pass
    elif val is not None:
        val = [val]
    else:
        val = []
    return val
