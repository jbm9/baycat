import os

METADATA_DIRNAME = ".baycat"

def bc_path_join(*args):
    '''Join paths; like os.path.join, but without magic

    Specifically, os.path.join() will truncate any time it's handed an
    absolute path.  So `os.path.join('foo', '/bar') == '/bar'`, which
    is distinctly not what we want here.
    '''
    result = '/'.join(args)
    while '//' in result:
        result = result.replace('//', '/')
    return result
