from __future__ import print_function

import os
import sys
import json
import string
import argparse as ap
import subprocess
from os import path
from collections import namedtuple, OrderedDict

import jedi

SOURCE_FILE_BATCH = 10


def log(msg):
    if _verbose:
        sys.stderr.write(msg + '\n')


def error(msg):
    if not _quiet:
        sys.stderr.write(msg + '\n')


def graph_wrapper(dir_, pretty=False, nSourceFilesTrunc=None):
    os.chdir(dir_)          # set working directory to be source directory

    source_files = get_source_files('.')
    if nSourceFilesTrunc is not None:
        source_files = source_files[:nSourceFilesTrunc]

    all_data = {'Defs': [], 'Refs': []}
    for i in range(0, len(source_files), SOURCE_FILE_BATCH):
        log('processing source files %d to %d of %d' % (i, i + SOURCE_FILE_BATCH, len(source_files)))
        batch = source_files[i:i + SOURCE_FILE_BATCH]

        args = ["python", "-m", "grapher.graph", "--dir", "."]
        if _verbose:
            args.append('--verbose')
        if _quiet:
            args.append('--quiet')
        if pretty:
            args.append('--pretty')
        args.append('--files')
        args.extend(batch)
        p = subprocess.Popen(args, stdout=subprocess.PIPE)
        out, err = p.communicate()
        if err is not None:
            sys.stderr.write(err)

        data = json.loads(out.decode('utf-8'))
        all_data['Defs'].extend(order_dict(dct) for dct in data['Defs'])
        all_data['Refs'].extend(order_dict(dct) for dct in data['Refs'])

    json_indent = 2 if pretty else None
    print(json.dumps(all_data, indent=json_indent))


def order_dict(dct):
    return OrderedDict(sorted(dct.items(), key=lambda x: x[0]))


def graph(dir_, source_files, pretty=False):
    os.chdir(dir_)          # set working directory to be source directory

    jedi.cache.never_clear_cache = True  # never clear caches, because running in batch

    modules_and_files = [(filename_to_module_name(f), f) for f in source_files]

    defs = []
    refs = []
    for file in source_files:
        d, r = get_defs_refs(file)
        defs += d
        refs += r

    # Add module/package defs
    for module, filename in modules_and_files:
        defs.insert(0, Def(
            Path=module.replace('.', '/'),
            Kind='module',
            Name=module.split('.')[-1],
            File=filename,
            DefStart=0,
            DefEnd=0,
            Exported=True,
            Docstring='',           # TODO: extract module/package-level doc
            Data=None,
        ))

    # De-duplicate definitions (local variables may be defined in more than one
    # place). Could do something smarter here, but for now, just take the first
    # definition that appears. (References also point to the first definition.)
    unique_defs = []
    unique_def_paths = set()
    for def_ in defs:
        if not def_.Path in unique_def_paths:
            unique_defs.append(def_)
            unique_def_paths.add(def_.Path)

    # Self-references, dedup
    unique_refs = []
    unique_ref_keys = set()
    for def_ in unique_defs:
        ref = Ref(
            DefPath=def_.Path,
            DefFile=path.abspath(def_.File),
            Def=True,
            File=def_.File,
            Start=def_.DefStart,
            End=def_.DefEnd,
            ToBuiltin=False,
        )
        ref_key = (ref.DefPath, ref.DefFile, ref.File, ref.Start, ref.End)
        if ref_key not in unique_ref_keys:
            unique_ref_keys.add(ref_key)
            unique_refs.append(ref)
    for ref in refs:
        ref_key = (ref.DefPath, ref.DefFile, ref.File, ref.Start, ref.End)
        if ref_key not in unique_ref_keys:
            unique_ref_keys.add(ref_key)
            unique_refs.append(ref)

    json_indent = 2 if pretty else None
    print("sys: ", unique_defs[0], file=sys.stderr)
    print(json.dumps([order_dict(d.__dict__) for d in unique_defs][:4]), file=sys.stderr)
    # Use OrderedDict to have reproducible text outputs.
    dct = {'Defs': [order_dict(d.__dict__) for d in unique_defs],
           'Refs': [order_dict(r.__dict__) for r in unique_refs]}
    print(json.dumps(order_dict(dct), indent=json_indent))


def get_source_files(dir_):
    source_files = []
    for dirpath, dirnames, filenames in os.walk(dir_):
        rel_dirpath = os.path.relpath(dirpath, dir_)
        for filename in filenames:
            if os.path.splitext(filename)[1] == '.py':
                source_files.append(os.path.normpath(os.path.join(rel_dirpath, filename)))
    return source_files


def get_defs_refs(file_path):
    defs, refs = [], []

    # Get a clean UTF8 source.
    linecoler = LineColToOffConverter(jedi.Script(path=file_path).source)
    names = jedi.names(path=file_path, all_scopes=True, references=True)
    for name in names:
        if name.is_definition():
            def_ = jedi_def_to_def(name, file_path, linecoler)
            defs.append(def_)
        else:
            try:
                full_name = full_name_of_def(name, from_ref=True)
                if full_name == '':
                    raise Exception('full_name is empty')
                start = linecoler.convert(name.line, name.column)
                refs.append(Ref(
                    DefPath=full_name.replace('.', '/'),
                    DefFile=path.relpath(name.module_path),
                    Def=False,
                    File=file_path,
                    Start=start,
                    End=start + len(name.name),
                    ToBuiltin=name.in_builtin_module(),
                ))
            except Exception as e:
                error('failed to convert ref (%s) in source file %s: %s'
                      % (name, file_path, e))

    return defs, refs


def jedi_def_to_def(def_, source_file, linecoler):
    full_name = full_name_of_def(def_)

    start = linecoler.convert(def_.line, def_.column)

    print(full_name, file=sys.stderr)
    return Def(
        Path=full_name.replace('.', '/'),
        Kind=def_.type,
        Name=def_.name,
        File=path.relpath(def_.module_path),
        DefStart=start,
        DefEnd=start + len(def_.name),
        Exported=True,          # TODO: not all vars are exported
        Docstring=def_.docstring(),
        Data=None,
    )


def full_name_of_def(def_, from_ref=False):
    # TODO: This function
    # - currently fails for tuple assignments (e.g., 'x, y = 1, 3')
    # - doesn't distinguish between m(module).n(submodule) and m(module).n(contained-variable)

    if def_.in_builtin_module():
        return def_.name

    if def_.type == 'statement':
        # kludge for self.* definitions
        if def_.parent().type == 'function' and def_.name == 'self':
            parent = def_.parent()
            while parent.type != 'class':
                parent = parent.parent()
            full_name = ('%s.%s' % (parent.full_name, def_.name))
        else:
            full_name = ('%s.%s' % (def_.full_name, def_.name))
    elif def_.type == 'param':
        full_name = ('%s.%s' % (def_.full_name, def_.name))
    else:
        full_name = def_.full_name

    module_path = def_.module_path
    if from_ref:
        module_path = abs_module_path_to_relative_module_path(module_path)

    supermodule = supermodule_path(module_path).replace('/', '.')

    # definition definitions' full_name property contains only the promixal module, so we need to add back the parent
    # module components. Luckily, the module_path is relative in this case.
    return path.join(supermodule, full_name)


def supermodule_path(module_path):
    if path.basename(module_path) == '__init__.py':
        return path.dirname(path.dirname(module_path))
    return path.dirname(module_path)


def abs_module_path_to_relative_module_path(module_path):
    relpath = path.relpath(module_path)  # relative from pwd (which is set in main)
    if not relpath.startswith('..'):
        return relpath
    components = module_path.split(os.sep)
    pIdx = -1
    for i, cmpt in enumerate(components):
        if cmpt in ['site-packages', 'dist-packages']:
            pIdx = i
            break
    if pIdx != -1:
        return path.join(*components[i + 1:])

    for i, cmpt in enumerate(components):
        if cmpt.startswith('python'):
            pIdx = i
            break
    if pIdx != -1:
        return path.join(*components[i + 1:])
    raise Exception("could not convert absolute module path %s to relative module path" % module_path)


Def = namedtuple('Def', ['Path', 'Kind', 'Name', 'File', 'DefStart', 'DefEnd', 'Exported', 'Docstring', 'Data'])
Ref = namedtuple('Ref', ['DefPath', 'DefFile', 'Def', 'File', 'Start', 'End', "ToBuiltin"])


def filename_to_module_name(filename):
    if path.basename(filename) == '__init__.py':
        return path.dirname(filename).replace('/', '.')
    return path.splitext(filename)[0].replace('/', '.')


class LineColToOffConverter(object):
    def __init__(self, source):
        source_lines = source.split('\n')
        cumulative_off = [0]
        for line in source_lines:
            cumulative_off.append(cumulative_off[-1] + len(line) + 1)
        self._cumulative_off = cumulative_off

    # Converts from (line, col) position to byte offset. line is 1-indexed, col is 0-indexed
    def convert(self, line, column):
        line = line - 1  # convert line to 0-indexed
        if line >= len(self._cumulative_off):
            return None, 'requested line out of bounds %d > %d' % (line + 1, len(self._cumulative_off) - 1)
        return self._cumulative_off[line] + column


if __name__ == '__main__':
    argser = ap.ArgumentParser(description='graph.py is a command that dumps all Python definitions and references found in code rooted at a directory')
    argser.add_argument('--dir', help='path to root directory of code')
    argser.add_argument('--files', help='path code files', nargs='+')
    argser.add_argument('--pretty', help='pretty print JSON output', action='store_true', default=False)
    argser.add_argument('--verbose', help='verbose', action='store_true', default=False)
    argser.add_argument('--quiet', help='quiet', action='store_true', default=False)
    argser.add_argument('--maxfiles', help='maximum number of files to process', default=None, type=int)
    args = argser.parse_args()

    _verbose, _quiet = args.verbose, args.quiet
    if args.files is not None and len(args.files) > 0:
        graph(args.dir, args.files, pretty=args.pretty)
    elif args.dir is not None and args.dir != '':
        graph_wrapper(args.dir, pretty=args.pretty, nSourceFilesTrunc=args.maxfiles)
    else:
        error('target directory must not be empty')
        sys.exit(1)
