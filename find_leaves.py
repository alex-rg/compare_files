#!/usr/bin/env python

"""
Find list of LFNs stored on a given DIRAC SE. 
"""

import sys
import os
import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.List import breakListIntoChunks
from DIRAC import gLogger

from multiprocessing.pool import ThreadPool
from multiprocessing import Manager
from random import shuffle


class dummyContextManager:
    def __enter__(self):
        return None
    def __exit__(self, *args):
        pass


def doLs( directory, fc ):
  dirs = []
  files = []

  print("ls of {0}".format(directory), file=sys.stderr)
  res = fc.listDirectory(directory, False)
  if not res['OK']:
    gLogger.error( "Can not list directory %s: %s" % (directory, res['Message']) )

  subdirs = res['Value']['Successful'][directory]['SubDirs']
  for key in  subdirs:
    if subdirs[key]:
      dirs.append(key)

  subfiles = res['Value']['Successful'][directory]['Files']
  for key in  subfiles:
    if subfiles[key]:
      files.append(key)

  return (dirs, files)


def getDirList( directory, depth, fc, exclude=None ):
  resd = []
  resf = []

  if depth == 0 :
    resd.append(directory)
  elif depth > 0 :
    (dirs, files) = doLs( directory, fc )
    resf = resf + files
    for f in dirs:
      if exclude is None or f not in exclude:
        (tdirs, tfiles) = getDirList( f, depth-1, fc, exclude )
        if tdirs:
          resd = resd + tdirs
        else:
          resd.append(f)
        resf = resf + tfiles
  else:
    gLogger.error("getDirList: depth is less than zero")
    DIRAC.exit(-1)
  return ( resd, resf )


def findFiles( directory, fc, se, lock ):
  print("find in {0}".format(directory), file=sys.stderr)
  res = fc.getMetadataFields()
  if not res['OK']:
    gLogger.error( 'Can not access File Catalog:', res['Message'] )
    DIRAC.exit( -1 )
  typeDict = res['Value']['FileMetaFields']
  typeDict.update( res['Value']['DirectoryMetaFields'] )
  # Special meta tags
  typeDict.update( FILE_STANDARD_METAKEYS )

  mq = MetaQuery( typeDict = typeDict )
  res = mq.setMetaQuery( [ 'Path=%s' % directory, 'SE=%s' % se ] )
  if not res['OK']:
    gLogger.error( "Illegal metaQuery:", res['Message'] )
    DIRAC.exit( -1 )
  metaDict = res['Value']
  path = metaDict.pop( 'Path', directory )

  res = fc.findFilesByMetadata( metaDict, path )
  if not res['OK']:
    gLogger.error( 'Can not find files from directory {0}: {1}'.format(directory, res['Message']) )
    fres = None
  else:
    with lock:
      for f in res['Value']:
        print(f)
    fres = 1
  return fres

def processDirectories( directories, fc, se, lock ):
    failed = []
    files = []
    for dr in directories:
        res = findFiles(dr, fc, se, lock)
        if not res:
          failed.append(dr)
    return failed

def getFiles( directories, se, lock ):
    my_fc = FileCatalog()
    failed = processDirectories(directories, fc, se, lock)
    return failed


if __name__ == "__main__":

  Script.registerSwitch( '', 'exclude=', '    comma-separated list of the directories in FC that should not be searched' )
  Script.registerSwitch( '', 'Path=', '    path in FileCatalog to be searched' )
  Script.registerSwitch( '', 'depth=', '    depth to be used during directory tree split. 3 by default' )
  Script.registerSwitch( '', 'threads=', '    Number of threads to use for file dumping' )
  Script.registerSwitch( '', 'SE=', '    Dump files only from this storage elements' )
  #Script.setUsageMessage( '\n'.join(
  #      [
  #        __doc__.split( '\n' )[1],
  #        'Usage:',
  #        '  %s [options] --Path=<PATH> --SE=<SE>' % Script.scriptName,
  #      ]
  #    )
  #  )

  Script.parseCommandLine( ignoreErrors = True )

  from DIRAC.Resources.Catalog.FileCatalog import FileCatalog
  from DIRAC.DataManagementSystem.Client.MetaQuery import MetaQuery, FILE_STANDARD_METAKEYS
  from DIRAC.Resources.Catalog.FileCatalog import FileCatalog
  from DIRAC.Resources.Storage.StorageElement import StorageElement

  fc = FileCatalog()

  directory = '/lhcb'
  depth = 2
  rfiles = []
  se = 'RAL-RDST'
  nthreads = 3

  for opt, val in Script.getUnprocessedSwitches():
    if opt == 'Path':
      directory = val
    elif opt == 'exclude':
      exclude = val.split(',')
    elif opt == 'depth':
      depth = int(val)
    elif opt == 'SE':
      se = val
    elif opt == 'threads':
      nthreads = int(val)

  res = fc.isDirectory(directory)
  if not res['OK']:
    gLogger.error("Can not check directory %s, %s" % (directory,res['Message']) )
    DIRAC.exit( -1 )

  if not res['Value']['Successful'][directory]:
    gLogger.error("Path must be a directory, but %s is not" % directory)
    DIRAC.exit( -1 )

  ( dirs, files ) = getDirList(directory, depth, fc)
  shuffle(dirs)
  if nthreads > 1:
    pool = ThreadPool(nthreads)
    lst_size = len(dirs) // 3 + 1
    with Manager() as manager:
      lock = manager.Lock()
      res = pool.starmap(getFiles, ((x, se, lock) for x in breakListIntoChunks(dirs, lst_size)))
  else:
    dummy_lock = dummyContextManager()
    res = [getFiles(dirs, se, dummy_lock)]

  print("\n\n\nUnchecked files")
  for f in files:
    print(f)

  print("\n\n\nFAILED:")
  for lst in res:
    for d in lst:
      print(d)
