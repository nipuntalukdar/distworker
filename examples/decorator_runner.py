import json
from time import sleep

from distworker.loop import distwork, start_loop


@distwork
def fun(x):
    return x + 1


configs = {}
with open("/usr/share/distworker/client_configs.json", "r") as fp:
    configs.update(json.load(fp))


start_loop(configs)
for i in range(100):
    a = fun(i)
    print(f"Result is {a.result()}")
sleep(5)
