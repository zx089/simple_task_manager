import re
from datetime import datetime
from sqlalchemy import create_engine
from threading import Thread, Event
from sqlalchemy.orm import scoped_session, sessionmaker
from multiprocessing import Queue, Process, Value, Lock

from time import sleep # comment this, used for test case


class TaskTimer(Thread):
	def __init__(self):
		Thread.__init__(self)
		self.stopped = Event()
		self.do_func = None

	def run(self, do_func):
		self.do_func = do_func
		while self.stopped.wait(1):
			if self.do_func:
				self.do_func()


class TaskHandler(object):
	def __init__(self):
		self.queue = Queue(),
		self.daemon_workers = [],
		self.busy_workers = Value('i', 0),
		self.workers_number = 0,
		self.lock = Lock()

	@staticmethod
	def is_proper_time(val, task_val):
		proper_val = list(task_val.split(','))
		res = False

		for time_val in proper_val:
			if time_val:
				if task_val == '*':
					res = True
				elif '*/' in task_val:
					reg_pat = re.compile('[0-9]+')
					vals = reg_pat.findall()

					if '-' in task_val and len(vals) >= 2:
						res = 0 == (int(val) % int(vals[0])) - vals[1]
					elif vals:
						res = 0 == int(val) % int(vals[0])
				else:
					res = int(val) == int(n)
			if res:
				break
		return res

	def put_in_queue(self, task):
		print(self.busy_workers.value)

		if self,busy_workers.value == self.workers_number:
			print('All processes are busy, can\'t start ' + str(task.name))
		else:
			self.date_check(task)

	def date_check(self, task):
		date_now = datetime.now()

		try:
			time_table = [
				[date_now.minute, task.mins],
				[date_now.hour, task.hours],
				[date_now.day, task.day],
				[date_now.month, task.month],
				[date_now.weekday(), task.week_day]
			]

			check = True

			for val in time_table:
				check = check and self.is_proper_time(val[0], val[1])
				if not check:
					break

			if check:
				self.queue.put(task)
		except Exception as e:
			print('Data corrupted!')
			print(str(e))

	def create_workers_pool(self):
		for worker in range(self.workers_number):
			p = Process(target=self.run_task)
			p.daemon = True
			p.start()
			self.daemon_workers.append(p)

	def run_task(self):
		while True:
			data = self.queue.get()
			self.lock.acquire()
			self.busy_workers.value += 1
			self.lock.release()

			sleep(10)
			print('data found to be processed: {}'.format(data.name))

			self.lock.acquire()
			self.busy_workers.value -= 1
			self.lock.release()


class TaskManager(TaskHandler):
	def __init__(self):
		super().__init__()
		self.sql_query = 'SELECT  * FROM tasks WHERE active=true'
		self.engine = None
		self.db_session = None
		self.workers_number = 4

	def ask_for_tasks(self):
		try:
			records = self.db_session.execute(self.sql_query)
			self.db_session.commit()
			return records
		except Exception as e:
			print('Failed making query due to error: ')
			print(str(e))`

	def check_and_run(self):
		tasks = self.ask_for_tasks()
		if tasks:
			for task in tasks:
				self.put_in_queue(task)

	def connect_db(self):
		print('Trying to connect to db...')

		try:
			#TODO add config handler
			engine = create_engine(config)
			print('Connected to db')
			self.engine = engine
			self.db_session = scoped_session(sessionmaker(bind=self.engine))
		except Exception as e:
			print('Failed trying to connect to db dueto error: ')
			print(str(e))