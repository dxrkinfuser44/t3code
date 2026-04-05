const TEST_GIT_ENV_SENTINEL = "T3_SERVER_TEST_GIT_CONFIG_APPLIED";

function appendGitConfigOverride(key: string, value: string) {
  const countRaw = process.env.GIT_CONFIG_COUNT;
  const currentCount = Number.parseInt(countRaw ?? "0", 10);
  const safeCount = Number.isFinite(currentCount) && currentCount >= 0 ? currentCount : 0;

  process.env[`GIT_CONFIG_KEY_${safeCount}`] = key;
  process.env[`GIT_CONFIG_VALUE_${safeCount}`] = value;
  process.env.GIT_CONFIG_COUNT = String(safeCount + 1);
}

if (process.env[TEST_GIT_ENV_SENTINEL] !== "1") {
  // Force unsigned commits/tags for ephemeral test repos, independent of host git agent setup.
  appendGitConfigOverride("commit.gpgsign", "false");
  appendGitConfigOverride("tag.gpgSign", "false");
  process.env[TEST_GIT_ENV_SENTINEL] = "1";
}
