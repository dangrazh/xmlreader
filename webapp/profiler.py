import cProfile, pstats, io

from timeit import default_timer as timer
import sys
import gc


def profile(fnc):

    """A decorator that uses cProfile to profile a function"""

    def inner(*args, **kwargs):

        pr = cProfile.Profile()
        pr.enable()
        retval = fnc(*args, **kwargs)
        pr.disable()
        s = io.StringIO()
        sortby = "cumulative"
        ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
        ps.print_stats()
        print(s.getvalue())
        return retval

    return inner


class StopWatch:
    """A class that makes timing of an operation easy"""

    def __init__(self, run_label=None, start_run=True):
        """Parameters: start_run -> wether to start the clock on creation of the class
        run_label -> an optional label for what is being timed
        """
        self.timer_start = None

        if run_label:
            self.run_label = f" with {run_label} "
        else:
            self.run_label = " "

        if start_run:
            self.start_run()

    def start_run(self):
        """Start the timer"""
        self.timer_start = timer()

    def time_run(self):
        """Take the time and return a formatted string with timing info"""
        seconds = timer() - self.timer_start
        return f"Done{self.run_label}in {seconds} seconds"


def get_actualsizeof(input_obj):
    memory_size = 0
    mem_size_MiB = 0.0

    ids = set()
    objects = [input_obj]
    while objects:
        new = []
        for obj in objects:
            if id(obj) not in ids:
                ids.add(id(obj))
                memory_size += sys.getsizeof(obj)
                new.append(obj)
        objects = gc.get_referents(*new)

    mem_size_MiB = memory_size / (1024 * 1024)

    return mem_size_MiB
