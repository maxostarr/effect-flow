import { expect, test } from "bun:test";
import { join } from "node:path";

const session = async (mode: "crash" | "resume", filename: string, runId: string) => {
  const proc = Bun.spawn(
    ["bun", join(import.meta.dir, "fixtures/durable-session.ts"), mode, filename, runId],
    { stdout: "pipe", stderr: "pipe" },
  );
  const [stdout, stderr, code] = await Promise.all([
    new Response(proc.stdout).text(),
    new Response(proc.stderr).text(),
    proc.exited,
  ]);
  return { stdout: stdout.trim(), stderr, code };
};

test("Run paused on a delay resumes in a new process after a crash", async () => {
  const filename = `/tmp/opencode/durable-${crypto.randomUUID()}.db`;
  const runId = `run-${crypto.randomUUID()}`;

  const crashed = await session("crash", filename, runId);
  expect(crashed.code).toBe(0);

  const resumed = await session("resume", filename, runId);
  expect(resumed.stderr).toBe("");
  expect(JSON.parse(resumed.stdout)).toEqual({ runId, nodes: ["n1", "n2", "n3"] });
});
