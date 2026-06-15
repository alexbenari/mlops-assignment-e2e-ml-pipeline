## Legend
Hi my MLOps colleague,

As you know, we reserch coding agents, trying to make them better via LLMs or harness. I found a simple research-friendly agent: `mini-swe-agent`.

I managed to run this `mini-swe-agent` on a VM with Docker.
> BTW, I checked the source code [github.com/SWE-agent/mini-swe-agent](https://github.com/SWE-agent/mini-swe-agent), it's really simple, close to 100 lines of code -- don't hestitate to check it out yourself / ask Codex to explain it to you. `git clone git@github.com:SWE-agent/mini-swe-agent.git`  I've never thought that these coding agents are sooo simple in a nutshell!

Check my script `mini-swe-bench-single.sh` -- it runs `mini-swe-agent` on a single SWE-bench instance. It uses `moonshotai/Kimi-K2.6`, make sure to set your `NEBIUS_API_KEY`.

Try it yourself, see how it works. Once it completes, you should find `trajectory.json` file with all of the steps, agent configuration, patch, etc.

`trajectory.json` could be hard to read by eye, this tool could help: `mini-e inspect trajectory.json`.

I also managed to run this `mini-swe-agent` in batch mode over a couple of SWE-bench tasks: `bash mini-swe-bench-batch.sh`. I writes some trajectories and logs to `trajectories/`.

As the next step, I needed to run SWE-bench evaluation. I checked `mini-swe-agent`'s [docs](https://mini-swe-agent.com/latest/usage/swebench/#__tabbed_2_2) on this -- they offer some recipe.
```bash
python -m swebench.harness.run_evaluation \
    --dataset_name princeton-nlp/SWE-bench_Verified \
    --predictions_path preds.jsonl \
    --max_workers <num_workers> \
    --run_id <run_id>
```

>  FYI, you can inspect their code if you wish: https://github.com/SWE-bench/SWE-bench

I installed this SWE-bench package and managed to run evals, see: `swe-bench-eval.sh`.

To make your life easier, I saved the sample outputs for my runs in `sample/`.

## Task
