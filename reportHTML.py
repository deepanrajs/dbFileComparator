import json

report_template = "./Report_Template.html"


def create_html_report(s_record_count, t_record_count, matched_count, unmatched_count, s_only_count, t_only_count,
                       h_report, output_directory, s_table, t_table, Report_Path):
    # print("Source_transactions",  s_record_count, "Target_transactions",  t_record_count,
    #                "SourceOnly_count",  s_only_count, "TargetOnly_count",  t_only_count,
    #                "matched_transactions_count",  matched_count, "unmatched_transactions_count",  unmatched_count,
    #                "source",  s_table, "target",  t_table, "report_path",  Report_Path)

    report_dict = {"Source_transactions": s_record_count, "Target_transactions": t_record_count,
                   "SourceOnly_count": s_only_count, "TargetOnly_count": t_only_count,
                   "matched_transactions_count": matched_count, "unmatched_transactions_count": unmatched_count,
                   "source": s_table, "target": t_table, "report_path": Report_Path}
    report_dict['report_path'] = report_dict['report_path'].replace("\\", "/")
    # print(report_dict)

    json_object = json.dumps(report_dict)
    template_file = open(report_template, "r")
    template_data = ""
    for data in template_file:
        template_data = template_data + data
    template_file.close()
    report_file = open(output_directory + "/" + h_report, "w")
    report_file.write(template_data.replace("dynamic_value", "'" + json_object + "'"))
