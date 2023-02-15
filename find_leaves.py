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


def getDirList( directory, depth, fc, exclude ):
  resd = []
  resf = []

  if depth == 0 :
    resd.append(directory)
  elif depth > 0 :
    (dirs, files) = doLs( directory, fc )
    resf = resf + files
    for f in dirs:
      if f not in exclude:
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




if __name__ == "__main__":

  Script.registerSwitch( '', 'exclude=', '    comma-separated list of the directories in FC that should not be searched' )
  Script.registerSwitch( '', 'Path=', '    path in FileCatalog to be searched' )
  Script.registerSwitch( '', 'depth=', '    depth to be used during directory tree split. 3 by default' )
  Script.registerSwitch( '', 'threads=', '    Number of threads to use for file dumping' )
#  Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
#                                       'Usage:',
#                                       '  %s [options] --Path=<PATH> --SE=<SE>' % Script.scriptName,
#                                       ] )
#                          )

  Script.parseCommandLine( ignoreErrors = True )

  from DIRAC.Resources.Catalog.FileCatalog import FileCatalog
  from DIRAC.DataManagementSystem.Client.MetaQuery import MetaQuery, FILE_STANDARD_METAKEYS
  from DIRAC.Resources.Catalog.FileCatalog import FileCatalog
  from DIRAC.Resources.Storage.StorageElement import StorageElement

  fc = FileCatalog()

  directory = '/lhcb'
  exclude = [ '/lhcb/user', '/lhcb/debug' ]
  depth = 2
  rfiles = []

  for opt, val in Script.getUnprocessedSwitches():
    if opt == 'Path':
      directory = val
    elif opt == 'exclude':
      exclude = val.split(',')
    elif opt == 'depth':
      depth = int(val)
  
  res = fc.isDirectory(directory)
  if not res['OK']:
    gLogger.error("Can not check directory %s, %s" % (directory,res['Message']) )
    DIRAC.exit( -1 )

  if not res['Value']['Successful'][directory]:
    gLogger.error("Path must be a directory, but %s is not" % directory)
    DIRAC.exit( -1 )
    
  ( dirs, files ) = getDirList(directory, depth, fc, exclude)
  print("DIRS:")
  for d in dirs:
    print(d)

  print("\n\n\nFILES:")
  for f in files:
    print(f)
# 
