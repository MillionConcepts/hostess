import fire

if __name__ == "__main__":
    from killscreen.controller.solicit_tasks import task_solicitation_server
    fire.Fire(task_solicitation_server)
