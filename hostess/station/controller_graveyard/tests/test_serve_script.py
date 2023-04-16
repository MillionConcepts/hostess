if __name__ == "__main__":
    from hostess.controller.serve_tasks import task_server
    tasks = {i: {"status": "ready"} for i in range(100)}
    task_server(tasks=tasks, run=True)
