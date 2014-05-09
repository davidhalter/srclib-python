package python

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"text/template"

	"sourcegraph.com/sourcegraph/srcgraph/config"
	"sourcegraph.com/sourcegraph/srcgraph/container"
	"sourcegraph.com/sourcegraph/srcgraph/graph"
	"sourcegraph.com/sourcegraph/srcgraph/grapher2"
	"sourcegraph.com/sourcegraph/srcgraph/repo"
	"sourcegraph.com/sourcegraph/srcgraph/task2"
	"sourcegraph.com/sourcegraph/srcgraph/unit"
)

func init() {
	grapher2.Register(&FauxPackage{}, grapher2.DockerGrapher{defaultPythonEnv})
}

const srcRoot = "/src"
const stdLibRepo = repo.URI("hg.python.org/cpython")

var builtinPrefixes = map[string]string{"sys": "sys", "os": "os", "path": "os/path"}

var grapherDockerfileTemplate = template.Must(template.New("").Parse(`FROM dockerfile/java
RUN apt-get update
RUN apt-get install -qy curl
RUN apt-get install -qy git
RUN apt-get install -qy {{.Python}}
RUN ln -s $(which {{.Python}}) /usr/bin/python
RUN curl https://raw.githubusercontent.com/pypa/pip/cdee19c77cf6514d42e2d1b7134f10b8ed36b63a/contrib/get-pip.py > /tmp/get-pip.py
RUN python /tmp/get-pip.py
RUN pip install virtualenv

# install python3 version
RUN add-apt-repository ppa:fkrull/deadsnakes > /dev/null  # (TODO: kinda sketchy)
RUN apt-get update
RUN apt-get install -qy {{.Python3}}
RUN rm /usr/bin/python3
RUN ln -s $(which {{.Python3}}) /usr/bin/python3

# PyDep
RUN pip install git+git://github.com/sourcegraph/pydep@0.0

# Pysonar
RUN apt-get install -qy maven
RUN git clone --depth 1 --branch 0.0.1 https://github.com/sourcegraph/pysonar2.git /pysonar2
WORKDIR /pysonar2
RUN mvn clean package
WORKDIR /

# Set up virtualenv (will contain dependencies)
RUN virtualenv /venv
`))

var grapherDockerCmdTemplate = template.Must(template.New("").Parse(`
/venv/bin/pip install {{.SrcDir}} 1>&2 || /venv/bin/pip install -r {{.SrcDir}}/requirements.txt 1>&2;

# Compute requirements
REQDATA=$(pydep-run.py {{.SrcDir}});

# Compute graph
echo 'Running graphing step...' 1>&2;
mkfifo /tmp/pysonar.err;
cat -v /tmp/pysonar.err &> /dev/null &  # bug: container hangs if we print this output
GRAPHDATA=$(java {{.JavaOpts}} -classpath /pysonar2/target/pysonar-2.0-SNAPSHOT.jar org.yinwang.pysonar.JSONDump {{.SrcDir}} '{{.IncludePaths}}' '' 2>/tmp/pysonar.err);
echo 'Graphing done.' 1>&2;

echo "{ \"graph\": $GRAPHDATA, \"reqs\": $REQDATA }";
`))

func (p *pythonEnv) grapherDockerfile() []byte {
	var buf bytes.Buffer
	grapherDockerfileTemplate.Execute(&buf, struct {
		Python  string
		Python3 string
		SrcDir  string
	}{
		Python:  p.PythonVersion,
		Python3: p.Python3Version,
		SrcDir:  srcRoot,
	})
	return buf.Bytes()
}

func (p *pythonEnv) stdLibDir() string {
	return fmt.Sprintf("/usr/lib/%s", p.PythonVersion)
}

func (p *pythonEnv) sitePackagesDir() string {
	return filepath.Join("/venv", "lib", p.PythonVersion, "site-packages")
}

func (p *pythonEnv) grapherCmd() []string {
	javaOpts := os.Getenv("PYGRAPH_JAVA_OPTS")
	inclpaths := []string{srcRoot, p.stdLibDir(), p.sitePackagesDir()}

	var buf bytes.Buffer
	grapherDockerCmdTemplate.Execute(&buf, struct {
		JavaOpts     string
		SrcDir       string
		IncludePaths string
	}{
		JavaOpts:     javaOpts,
		SrcDir:       srcRoot,
		IncludePaths: strings.Join(inclpaths, ":"),
	})
	return []string{"/bin/bash", "-c", buf.String()}
}

func (p *pythonEnv) BuildGrapher(dir string, unit unit.SourceUnit, c *config.Repository, x *task2.Context) (*container.Command, error) {
	return &container.Command{
		Container: container.Container{
			RunOptions: []string{"-v", dir + ":" + srcRoot},
			Dockerfile: p.grapherDockerfile(),
			Cmd:        p.grapherCmd(),
			Stderr:     x.Stderr,
			Stdout:     x.Stdout,
		},
		Transform: func(orig []byte) ([]byte, error) {
			var o rawGraphData
			err := json.Unmarshal(orig, &o)
			if err != nil {
				outPrefix := string(orig)
				if len(outPrefix) > 100 {
					outPrefix = outPrefix[0:100] + "..."
				}
				return nil, fmt.Errorf("could not unmarshal grapher output as JSON (%s): %s", err, outPrefix)
			}

			o2 := grapher2.Output{
				Symbols: make([]*graph.Symbol, 0),
				Refs:    make([]*graph.Ref, 0),
				Docs:    make([]*graph.Doc, 0),
			}

			selfrefs := make(map[graph.Ref]struct{})
			for _, psym := range o.Graph.Syms {
				sym, selfref, err := p.convertSym(psym, c, o.Reqs)
				if err != nil {
					return nil, fmt.Errorf("could not convert sym %+v: %s", psym, err)
				}

				if sym != nil {
					o2.Symbols = append(o2.Symbols, sym)
				}
				if selfref != nil {
					selfrefs[*selfref] = struct{}{}
					o2.Refs = append(o2.Refs, selfref)
				}
			}
			for _, pref := range o.Graph.Refs {
				if ref, err := p.convertRef(pref, c, o.Reqs); err == nil {
					if _, exists := selfrefs[*ref]; !exists {
						o2.Refs = append(o2.Refs, ref)
					}
				} else {
					log.Printf("  (warn) unable to convert reference %+v", pref)
				}
			}
			for _, pdoc := range o.Graph.Docs {
				doc, err := p.convertDoc(pdoc, c, o.Reqs)
				if err != nil {
					return nil, fmt.Errorf("could not convert doc %+v: %s", pdoc, err)
				}
				o2.Docs = append(o2.Docs, doc)
			}

			b, err := json.Marshal(o2)
			if err != nil {
				return nil, fmt.Errorf("Could not marshal graph JSON: %s", err)
			}
			return b, nil
		},
	}, nil
}

func (p *pythonEnv) convertSym(pySym *pySym, c *config.Repository, reqs []requirement) (sym *graph.Symbol, selfref *graph.Ref, err error) {
	symKey, err := p.pysonarSymPathToSymKey(pySym.Path, c, reqs)
	if err != nil {
		return
	}
	file, err := p.pysonarFilePathToFile(pySym.File)
	if err != nil {
		return
	}

	sym = &graph.Symbol{
		SymbolKey:    *symKey,
		Name:         pySym.Name,
		File:         file,
		DefStart:     pySym.DefStart,
		DefEnd:       pySym.DefEnd,
		Exported:     pySym.Exported,
		Callable:     callableSymbolKinds[pySym.Kind],
		Kind:         symbolKinds[pySym.Kind],
		SpecificKind: symbolSpecificKinds[pySym.Kind],
	}
	if pySym.Exported {
		components := strings.Split(string(sym.Path), "/")
		if len(components) == 1 {
			sym.SpecificPath = components[0]
		} else {
			// take the last 2 path components
			sym.SpecificPath = components[len(components)-2] + "." + components[len(components)-1]
		}
	} else {
		sym.SpecificPath = pySym.Name
	}
	if pySym.FuncData != nil {
		sym.TypeExpr = pySym.FuncData.Signature
	}
	if pySym.Kind == "MODULE" && strings.HasSuffix(pySym.File, "__init__.py") {
		sym.SpecificKind = Package
		sym.Kind = graph.Package
	}

	if sym.File != "" && pySym.IdentStart != pySym.IdentEnd {
		var symFile string
		symFile, err = p.pysonarFilePathToFile(pySym.File)
		if err != nil {
			return
		}
		selfref = &graph.Ref{
			SymbolRepo:     symKey.Repo,
			SymbolUnitType: symKey.UnitType,
			SymbolUnit:     symKey.Unit,
			SymbolPath:     symKey.Path,
			Def:            true,

			Repo:     symKey.Repo,
			UnitType: symKey.UnitType,
			Unit:     symKey.Unit,

			File:  symFile,
			Start: pySym.IdentStart,
			End:   pySym.IdentEnd,
		}
	}

	return
}

func (p *pythonEnv) convertRef(pyRef *pyRef, c *config.Repository, reqs []requirement) (*graph.Ref, error) {
	symKey, err := p.pysonarSymPathToSymKey(pyRef.Sym, c, reqs)
	if err != nil {
		return nil, err
	}
	refRepo, refFile, err := p.pysonarFilePathToRepoAndFile(pyRef.File, c, reqs)
	if err != nil {
		return nil, err
	}

	return &graph.Ref{
		SymbolRepo:     symKey.Repo,
		SymbolUnitType: symKey.UnitType,
		SymbolUnit:     symKey.Unit,
		SymbolPath:     symKey.Path,
		Def:            false,

		Repo:     refRepo,
		UnitType: unit.Type(&FauxPackage{}),
		Unit:     (&FauxPackage{}).Name(),

		File:  refFile,
		Start: pyRef.Start,
		End:   pyRef.End,
	}, nil
}

func (p *pythonEnv) convertDoc(pyDoc *pyDoc, c *config.Repository, reqs []requirement) (*graph.Doc, error) {
	// TODO: handle null byte (\x00) in doc body?
	symKey, err := p.pysonarSymPathToSymKey(pyDoc.Sym, c, reqs)
	if err != nil {
		return nil, err
	}
	docFile, err := p.pysonarFilePathToFile(pyDoc.File)
	if err != nil {
		return nil, err
	}
	return &graph.Doc{
		SymbolKey: *symKey,
		Format:    "", // TODO
		Data:      formatDocs(pyDoc.Body),
		File:      docFile,
		Start:     pyDoc.Start,
		End:       pyDoc.End,
	}, nil
}

func (p *pythonEnv) pysonarFilePathToFile(pth string) (string, error) {
	if newpath, err := filepath.Rel(srcRoot, pth); err == nil {
		return newpath, nil
	} else if newpath, err := filepath.Rel(p.sitePackagesDir(), pth); err == nil {
		return newpath, nil
	} else if newpath, err := filepath.Rel(p.stdLibDir(), pth); err == nil {
		return newpath, nil
	} else {
		return "", fmt.Errorf("Could not relativize file path %s", pth)
	}
}

func (p *pythonEnv) pysonarFilePathToRepoAndFile(pth string, c *config.Repository, reqs []requirement) (repo.URI, string, error) {
	if relpath, err := filepath.Rel(srcRoot, pth); err == nil {
		return c.URI, relpath, nil
	} else if relpath, err := filepath.Rel(p.sitePackagesDir(), pth); err == nil {
		var foundReq *requirement
	FindReq:
		for _, req := range reqs {
			for _, pkg := range req.Packages {
				pkgpath := strings.Replace(pkg, ".", "/", -1)
				if _, err := filepath.Rel(pkgpath, relpath); err == nil {
					foundReq = &req
					break FindReq
				}
			}
			for _, mod := range req.Modules {
				modpath := mod + ".py"
				if _, err := filepath.Rel(modpath, relpath); err == nil {
					foundReq = &req
					break FindReq
				}
			}
		}
		if foundReq == nil {
			return "", "", fmt.Errorf("Could not resolve repo URL for file path %s", pth)
		}
		return repo.MakeURI(foundReq.RepoURL), relpath, nil
	} else if relpath, err := filepath.Rel(p.stdLibDir(), pth); err == nil {
		return stdLibRepo, relpath, nil
	} else {
		return "", "", fmt.Errorf("Could not resolve repo URL for file path %s", pth)
	}
}

func (p *pythonEnv) pysonarSymPathToSymKey(pth string, c *config.Repository, reqs []requirement) (*graph.SymbolKey, error) {
	fauxUnit := &FauxPackage{}
	if relpath, err := filepath.Rel(srcRoot, pth); err == nil {
		return &graph.SymbolKey{
			Repo:     c.URI,
			UnitType: unit.Type(fauxUnit),
			Unit:     fauxUnit.Name(),
			Path:     graph.SymbolPath(relpath),
		}, nil
	} else if relpath, err := filepath.Rel(p.sitePackagesDir(), pth); err == nil {
		var foundReq *requirement
	FindReq:
		for _, req := range reqs {
			for _, pkg := range req.Packages {
				pkgpath := strings.Replace(pkg, ".", "/", -1)
				if _, err := filepath.Rel(pkgpath, relpath); err == nil {
					foundReq = &req
					break FindReq
				}
			}
			for _, mod := range req.Modules {
				modpath := mod
				if _, err := filepath.Rel(modpath, relpath); err == nil {
					foundReq = &req
					break FindReq
				}
			}
		}
		if foundReq == nil {
			return nil, fmt.Errorf("Could not find requirement matching path %s", pth)
		}

		return &graph.SymbolKey{
			Repo:     repo.MakeURI(foundReq.RepoURL),
			UnitType: unit.Type(fauxUnit),
			Unit:     fauxUnit.Name(),
			Path:     graph.SymbolPath(relpath),
		}, nil
	} else if relpath, err := filepath.Rel(p.stdLibDir(), pth); err == nil {
		return &graph.SymbolKey{
			Repo:     stdLibRepo,
			UnitType: unit.Type(fauxUnit),
			Unit:     fauxUnit.Name(),
			Path:     graph.SymbolPath(relpath),
		}, nil
	} else {
		for prefix, newPrefix := range builtinPrefixes {
			if strings.HasPrefix(pth, prefix) {
				return &graph.SymbolKey{
					Repo:     stdLibRepo,
					UnitType: unit.Type(fauxUnit),
					Unit:     fauxUnit.Name(),
					Path:     graph.SymbolPath(strings.Replace(pth, prefix, newPrefix, 1)),
				}, nil
			}
		}
		return nil, fmt.Errorf("Could not find requirement matching path %s", pth)
	}
}

type rawGraphData struct {
	Graph struct {
		Syms []*pySym
		Refs []*pyRef
		Docs []*pyDoc
	}
	Reqs []requirement
}

type pySym struct {
	Path       string
	Name       string
	File       string
	IdentStart int
	IdentEnd   int
	DefStart   int
	DefEnd     int
	Exported   bool
	Kind       string
	FuncData   *struct {
		Signature string
	} `json:",omitempty"`
}

type pyRef struct {
	Sym     string
	File    string
	Start   int
	End     int
	Builtin bool
}

type pyDoc struct {
	Sym   string
	File  string
	Body  string
	Start int
	End   int
}
