// webm -> gif. Prefers gifski (smaller files, nicer color) fed PNG frames
// extracted by ffmpeg; falls back to a pure-ffmpeg palettegen pipeline if
// gifski isn't installed.

import { spawnSync } from "node:child_process";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";

function run(cmd, args) {
  const r = spawnSync(cmd, args, { stdio: ["ignore", "pipe", "pipe"] });
  if (r.status !== 0) {
    throw new Error(`${cmd} failed (${r.status}): ${r.stderr?.toString() || r.error?.message}`);
  }
  return r;
}

function has(cmd) {
  return spawnSync("which", [cmd], { stdio: "ignore" }).status === 0;
}

/**
 * @param {object} o
 * @param {string} o.webm       source video
 * @param {string} o.out        output .gif path
 * @param {number} [o.fps]
 * @param {number} [o.width]    output width in px (height auto)
 * @param {number} [o.quality]  gifski quality 1-100
 * @param {number} [o.trimStart] seconds to cut from the start (drops boot/preamble)
 * @param {number} [o.duration]  cap output to this many seconds
 */
export function toGif({ webm, out, fps = 15, width = 1000, quality = 65, trimStart = 0, duration }) {
  fs.mkdirSync(path.dirname(out), { recursive: true });
  const trim = [];
  if (trimStart > 0) trim.push("-ss", String(trimStart));
  if (duration) trim.push("-t", String(duration));
  const vf = `fps=${fps},scale=${width}:-1:flags=lanczos`;

  if (has("gifski")) {
    const framesDir = fs.mkdtempSync(path.join(os.tmpdir(), "rkv-frames-"));
    try {
      run("ffmpeg", ["-y", "-loglevel", "error", ...trim, "-i", webm, "-vf", vf, path.join(framesDir, "f%05d.png")]);
      const frames = fs
        .readdirSync(framesDir)
        .filter((f) => f.endsWith(".png"))
        .sort()
        .map((f) => path.join(framesDir, f));
      if (frames.length === 0) throw new Error("no frames extracted from " + webm);
      // gifski halves the resolution by default (assumes 2x HiDPI input), so
      // pass --width explicitly to match the frames we already scaled.
      run("gifski", ["--width", String(width), "--quality", String(quality), "--fps", String(fps), "-o", out, ...frames]);
    } finally {
      fs.rmSync(framesDir, { recursive: true, force: true });
    }
  } else {
    // Fallback when gifski isn't installed. ffmpeg's palette pipeline produces
    // larger files for screen content, so here a gifsicle --lossy pass (if
    // present) earns its keep — unlike on gifski output, where it does little.
    const palette = out + ".palette.png";
    run("ffmpeg", ["-y", "-loglevel", "error", ...trim, "-i", webm, "-vf", `${vf},palettegen=stats_mode=diff`, palette]);
    run("ffmpeg", ["-y", "-loglevel", "error", ...trim, "-i", webm, "-i", palette, "-lavfi", `${vf}[x];[x][1:v]paletteuse=dither=bayer:bayer_scale=3`, out]);
    fs.rmSync(palette, { force: true });
    if (has("gifsicle")) run("gifsicle", ["--lossy=100", "--colors=128", "-O3", out, "-o", out]);
  }
  return out;
}
