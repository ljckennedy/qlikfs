from ast import match_case
import time
import json
import os, sys, argparse
from argparse import _SubParsersAction as subparsers
import pandas as pd
from qsaas.qsaas import Tenant

def ls(q, myspace):
    print("running ls")
    if myspace:
        cid = getspace(q, myspace)
        files = q.get('datafiles', params={"connectionId":cid})
    else:
         files = q.get('datafiles')

    tblPrint(files)
    
def getspace(q, myspace):
    # qlik raw get v1/datafiles/connections --query spaceid=5fa47573a0cfd40001bd5924
    # GET https://lkn-qlkaccount.ap.qlikcloud.com/api/v1/spaces?name=shared-data
    
    space=q.get('spaces', params={"name": myspace})
    try:
        #print(space[0]['id'])
        spid=space[0]['id']
    except:
        print("Space not found.  Aborting.")
        sys.exit(1)
    else:    
        print(space[0]['id'])
        spid=space[0]['id']
        conn=q.get('datafiles/connections', params={"spaceid": spid})
        print(conn[0]['id'])
        return conn[0]['id']

def tblPrint(qData):
    df = pd.DataFrame(qData)
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', None)
    pd.set_option('index', False)
    #print(df)
    print(df.to_string(index=False))
    print()


def cp():
    print("running cp")

def rm():
    print("running rm")


def main():
    parser = argparse.ArgumentParser(prog='qlikfs',description='Interact with Qlik Cloud as a filesystem')
    parser.add_argument('--tenant', '-t', 
      help='set name of tenant config file e.g. mytenant.json  defaults to tenant.json')
    subparsers = parser.add_subparsers(dest='command', help='command to run')
    subparsers.required = True 

    #  subparser for dump
    parser_ls = subparsers.add_parser('ls')
    parser_cp = subparsers.add_parser('cp')
    # add a required argument
    parser_ls.add_argument('--space', '-s', dest='myspace', type=str, required=False,  help='the space to list')

    parser_cp.add_argument('source', type=str,  help='source file')
    parser_cp.add_argument('dest', type=str,  help='destination file')

    args = parser.parse_args()
    print(args)
    print()

    if args.tenant:
        print("Connecting to %s" % args.tenant)
        q = Tenant(config="./"+args.tenant)
    else:
        q = Tenant(config="./tenant.json")
    tenants={}

    match args.command:
        case "ls":
            ls(q, args.myspace)
        case "cp":  
            cp(args.cmdopts)


if __name__ == '__main__':
    main()