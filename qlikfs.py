from ast import match_case
import time
import json
import os, sys, argparse
from argparse import _SubParsersAction as subparsers
#import prettytable
import pandas as pd
from qsaas.qsaas import Tenant
#from tabulate import tabulate

def ls(q, path):
    print("running ls")
    if path:
        cid = getPath(q, path)
        # ?connectionId=798ee261-9c4e-45c1-93e4-523e0439b6b1
        # , params={"resourceType":"app"}
        files = q.get('datafiles', params={"connectionId":cid})
    else:
         files = q.get('datafiles')

    tblPrint(files)
    
def getPath(q, path):
    # qlik raw get v1/datafiles/connections --query spaceid=5fa47573a0cfd40001bd5924
    # GET https://lkn-qlkaccount.ap.qlikcloud.com/api/v1/spaces?name=shared-data
    space=q.get('spaces', params={"name": path})
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
    parser_ls.add_argument('--path', '-p', dest='path', type=str, required=False,  help='the path to list')

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
            ls(q, args.path)
        case "cp":  
            cp(args.cmdopts)


if __name__ == '__main__':
    main()