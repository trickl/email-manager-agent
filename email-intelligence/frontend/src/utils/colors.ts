export function clamp01(x: number): number {
  return Math.max(0, Math.min(1, x));
}

/**
 * Usefulness score from unread ratio.
 * usefulness = 1 - unread_ratio
 */
export function usefulnessScore(unreadRatio: number): number {
  return clamp01(1 - (Number.isFinite(unreadRatio) ? unreadRatio : 0.5));
}

/**
 * Continuous HSL interpolation from red (0) to green (120).
 */
export function usefulnessColor(unreadRatio: number): string {
  const score = usefulnessScore(unreadRatio);
  // Perception tweak:
  // A linear mapping makes low-usefulness values look orange/brown for many common scores
  // (e.g. score ~0.2 => hue ~24). We apply a mild gamma curve so "low" stays redder.
  const gamma = 1.45;
  const hue = 120 * Math.pow(score, gamma); // 0 = red, 120 = green

  // Slightly higher saturation and lightness reads better on our dark UI.
  const sat = 82;
  const light = 46;
  // Use the widely supported comma-separated syntax to avoid black fallback
  // in browsers that don't support the CSS Color 4 space-separated form.
  return `hsl(${hue}, ${sat}%, ${light}%)`;
}

export function usefulnessBandLabel(unreadRatio: number): string {
  const u = usefulnessScore(unreadRatio);
  if (u >= 0.9) return "highly useful";
  if (u >= 0.7) return "useful";
  if (u >= 0.3) return "mixed";
  if (u >= 0.1) return "low value";
  return "very low value";
}

export function unreadBucketText(unreadRatio: number): string {
  const r = clamp01(Number.isFinite(unreadRatio) ? unreadRatio : 0);
  if (r === 1) return "all";
  if (r >= 0.9) return "almost all";
  if (r === 0) return "none";
  if (r <= 0.1) return "almost none";
  return "some";
}
