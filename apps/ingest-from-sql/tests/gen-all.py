import pandas as pd
from argparse import ArgumentParser
from pathlib import Path
import subprocess
import sys

from sqlalchemy import insert,update,Table,Column,Integer,LargeBinary,MetaData
from sqlalchemy import URL,create_engine
from sqlalchemy.ext.automap import automap_base
import sqlalchemy as sql


def generate(args):
    df = pd.read_csv(args.source)


    base_name=args.source
    has_link_column=False
    to_split=[]
    pk_col=None
    print("* Examining columns in input data")
    for c in df.columns:
        if c == args.link:
            has_link_column=True
        if c.startswith("blob_"):
            print(f"removing blob column: {c}")
            to_split.append(c)
        if c.startswith("pk_"):
            if pk_col is not None:
                raise Exception("already has pk")
            pk_col=c
            pk_rename=c[len("pk_"):]
            has_link_column=True
            args.link = pk_rename


    if pk_col is not None:
        df.rename(columns={pk_col:pk_rename},inplace=True)

    if to_split:

        if not has_link_column:
            raise Exception("Can't split blob column out, missing link column in data")

        table_name = Path(Path(args.source).name).stem
        gp = Path(args.generated_path)
        if not gp.exists():
            gp.mkdir()

        base_name = gp / '{}.csv'.format(table_name)
        blobs_name = gp / '{}_blobs.csv'.format(table_name)

        base = df.drop( labels=to_split,axis=1)
        non_blob_cols = list(base.columns.values)
        non_blob_cols.remove(args.link)
        print(f"Columns to remove for blob: {non_blob_cols}")
        blob_df = df.drop(labels=non_blob_cols,axis=1)
        base.to_csv(base_name, index=False)
        blob_df.to_csv( blobs_name,index=False)
 
    print(f"* Creating property data for {table_name}")
    cmd=f"csvsql --db {args.connection_string} --insert {base_name}" 
    print(f"  ** Running {cmd}")
    subproc = subprocess.run(cmd, shell=True) 
    if subproc.returncode != 0:
        raise Exception(f"Didn't add properly: {subproc}")

    if to_split:
        add_blobs( args, blobs_name , table_name )

    if pk_col is not None:
        engine = create_engine(args.connection_string)
        set_pk(engine,table_name,pk_rename)


def add_blobs(args,blob_csv_name,table_name):
    print(f"* Adding blob data to {table_name}")

    blob_columns = []
    data = pd.read_csv(blob_csv_name)
    for c in data.columns:
        if c.startswith("blob_"):
            blob_columns.append(c[len("blob_"):])
    engine = create_engine(args.connection_string)

    link_col = None
    table = None
    with engine.connect() as conn:
        meta = MetaData()
        meta.reflect(bind=engine)
        for t in meta.sorted_tables:
            if t.name == table_name: 
                table = t
                print(f"* Found existing column in database.") 
                for blob_col in blob_columns:
                    if not blob_col in t.columns:
                        print(f"  ** Adding Column for blob: {blob_col}")
                        col = Column(blob_col, LargeBinary)
                        add_column( conn, t.name, col )

                for c in t.columns:
                    if c.name == args.link:
                        link_col = c

    print(f"Link column is {link_col} input is {args.link}")
    if link_col is None:
        raise Exception("No link column found")
    with engine.connect() as conn:
        meta = MetaData()
        meta.reflect(bind=engine)
        for t in meta.sorted_tables:
            if t.name == table_name: 
                table = t

        if t is None:
            raise Exception(f"Couldn't find table {table_name} to add blobs to.")
        for idx,row in data.iterrows():
            for b in blob_columns:
                with open(row['blob_'+b],'rb') as fp:
                    blob_data = fp.read()
                insert_values = { b : blob_data }
                print(f"  ** Addding {row['blob_'+b]} for {row[args.link]}, size {len(blob_data)} to column {b}")
                insert_blobs = update(table).where(table.c.id==row[args.link]).values(**insert_values)
                conn.execute(insert_blobs.compile(engine))

        conn.commit()



    


# from SA#7300948
def add_column(engine,table_name,column_obj):
    column_name = column_obj.compile(dialect=engine.dialect)
    column_type = column_obj.type.compile(engine.dialect)
    sqls= "ALTER TABLE {0} ADD COLUMN {1} {2}".format(table_name, column_name, column_type)
    print(sqls)
    engine.execute(sql.text(sqls)) 
    engine.commit()

def set_pk(engine,table_name,column_name):
    with engine.connect() as conn:
        sqls= "ALTER TABLE {0} ADD PRIMARY KEY({1})".format(table_name, column_name)
        print(sqls)
        conn.execute(sql.text(sqls)) 
        conn.commit()


    # not working
    if False:
        from types import new_class
        #with engine.connect() as conn:
        meta = MetaData()
        meta.reflect(bind=engine,only=[table_name])
        Base = automap_base( metadata=meta)
        Modified = type('Modified',(Base,),
                {
                    '__tablename__': table_name,
                    '__table_args__': {'extend_existing':True},
                    column_name: Column(Integer, primary_key=True)
                }
                )
        print(f"Setting {column_name} as primary key")
        print(Modified)
        print(repr(Modified))
        stmt = Base.prepare()
        #stmt = Base.prepare(autoload_with=engine)
        #stmt = Modified.prepare(engine, reflect=True)
        print(stmt)
        print(repr(Modified))
        #conn.execute(stmt) 
        res = Modified.metadata.create_all(engine)
        print(res)

def get_opts():
    parser = ArgumentParser()
    parser.add_argument('-s','--source',required=True,help="Source CSV to load")
    parser.add_argument('-c','--connection-string',required=True,help="Connection string to db") 
    parser.add_argument('-g','--generated-path',default="generated/",
        help="Path to put generated output") 
    parser.add_argument('-l','--link',default="id",help="Column to link blob loads") 
    args= parser.parse_args()
    return args


if __name__ == '__main__':

    args = get_opts()
    generate(args)
