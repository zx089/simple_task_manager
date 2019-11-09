from task_utils import TaskManager, TaskTimer


def main():
	task_manager = TaskManager()
	task_timer = TaskTimer()

	task_manager.connect_db()
	task_manager.create_workers_pool()

	task_timer.run(task_manager.check_and_run)



if __name__ == '__main__':
	main()
