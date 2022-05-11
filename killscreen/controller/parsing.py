import re


def csv_sanitize(thing):
    return re.sub(r"[\n,]", "", str(thing)).strip()


def default_output_parser(process, out_list, err_list, exception=None):
    parsed = {}
    return_lines = [line for line in out_list if "return code: " in line]
    if len(return_lines) > 0:
        parsed["return_code"] = return_lines[0].replace("return code: ", "")
    elif process is not None:
        parsed["return_code"] = process.exit_code
    else:
        parsed["return_code"] = "unknown"
    err_line = None
    if len(err_list) > 0:
        # TODO: parse the exception more effectively. doesn't consistently
        #  work well if there are warnings or other at-exit messages.
        #  the below attempt is a bit of a hack. maybe actually add an
        #  explicit at-exit message in the pipeline itself?
        for err_line in reversed(err_list):
            if (
                ("warnings." in err_line)
                or ("resource_tracker" in err_line)
                or (":" not in err_line)
            ):
                continue
            break
    if err_line is not None:
        err_type, err_msg = err_line.split(":", maxsplit=1)
        parsed["pipeline_exc_type"] = err_type
        parsed["pipeline_exc_msg"] = err_msg
    if exception is not None:
        parsed["exc_type"] = type(exception)
        parsed["exc_msg"] = str(exception)
    return {k: csv_sanitize(str(v)) for k, v in parsed.items()}
