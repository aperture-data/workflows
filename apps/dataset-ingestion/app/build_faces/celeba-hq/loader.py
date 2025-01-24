# loader.py - Load CelebA-HQ results into ApertureDB

import os
from aperturedb.ConnectionDataCSV  import ConnectionDataCSV
from aperturedb.ImageDataCSV import ImageDataCSV
from aperturedb.PolygonDataCSV import PolygonDataCSV
from aperturedb import Utils
from aperturedb.ParallelLoader import ParallelLoader
from aperturedb.ParallelQuery import ParallelQuery

from finder import ImageExistsDataCSV,PolygonExistsDataCSV,ConnectionExistsDataCSV

import argparse
import logging

#logging.basicConfig(level=logging.DEBUG)

def parse_args():
	parser = argparse.ArgumentParser()
	parser.add_argument( '-s','--server',required=True)
	parser.add_argument( '-c','--connect',action='store_true')
	return parser.parse_args()

# map filename to parser
data = { 'images.adb.csv' : [ImageDataCSV, [ImageExistsDataCSV,  "celebahq_id", "celebahq_id" ] ],
	 'polygons.adb.csv' : [PolygonDataCSV,[ PolygonExistsDataCSV, "celebahqmask_id", "celebahqmask_id"]  ],
	 'connections.adb.csv' : [ConnectionDataCSV, [ConnectionExistsDataCSV, "connect_id","connect_id"]  ]
	}


if __name__ == '__main__':
	opts = parse_args()

	del data["images.adb.csv"]
	del data["polygons.adb.csv"]
	if not opts.connect:
		del data["connections.adb.csv"]

	os.environ["APERTUREDB_CONFIG"] = opts.server
	c = Utils.create_connector()
	for file in data.keys():
		if not os.path.exists( file ):
			raise Exception(f"Missing source csv: {file}")
	loader = ParallelLoader(c,dry_run=False)
	for file in data.keys():
		final_file = file
		(parser_class,exists_info) = data[file]
		if exists_info is not None:
			(tester_class,csv_column,db_column) = exists_info
			tester = tester_class(csv_column,db_column,file)
			querier = ParallelQuery(c)
			querier.query(tester,batchsize=100,numthreads=16,stats=True)
			missing = tester.get_missing_items()
			found = tester.get_found_items()
			final_file = f"{file}.filtered"
			if len(missing.index) == 0:
				print(f"No missing items in {file}, skipping load.")
				continue
			print(f"Missing items is {len(missing.index)}")
			print(f"Found items is {len(found.index)}")
			missing.to_csv( final_file, index=False )

		# create parser from associated csv class, and pass in filename.
		parser = data[file][0](final_file)

		print(file)
		print(parser)
		loader.ingest(parser)
