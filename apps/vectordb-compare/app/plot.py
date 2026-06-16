import os
import argparse

from dbeval import EvalTool


def main(params):

    # # Insertion
    # eval_name = os.path.join(params.output_folder, "descriptors_insert")
    # e_insert = EvalTool.EvalTool(eval_name)
    # e_insert.plot_all(folder=os.path.join(params.output_folder, "plots_insert"))

    # Query
    eval_name = os.path.join(params.output_folder, "knn_comp")
    e_knn = EvalTool.EvalTool(eval_name)
    e_knn.plot_n_res_scale = "x"
    e_knn.plot_query_time_scale = "x"
    e_knn.plot_query_throughput_scale = "x"
    e_knn.plot_n_res_throughput_scale = "x"
    e_knn.plot_all(folder=os.path.join(params.output_folder, "plots_knn"))


def get_args():
    obj = argparse.ArgumentParser()

    obj.add_argument('-output_folder', type=str,  default="output")
    obj.add_argument('-verbose',       type=bool, default=True)

    params = obj.parse_args()

    return params


if __name__ == "__main__":
    args = get_args()
    main(args)
