# Nest Recorder

A Home Assistant custom integration that continuously records Google Nest cameras locally and mirrors every segment to AWS S3 with a Glacier storage class for long-term archival.

- **Continuous 24/7 recording** on wired Nest cameras (RTSP via SDM).
- **Event-triggered clips** tagged from motion / person / doorbell / sound.
- **Battery Nest cameras** (WebRTC-only) are recorded whenever the SDM stream is active, via the go2rtc bridge bundled with Home Assistant.
- **Immediate S3 mirror** with `GLACIER`, `DEEP_ARCHIVE`, or `GLACIER_IR` storage class.
- **Restore-from-Glacier** service that polls and fires a persistent notification when the object is ready.
- **Media browser** tree grouped by camera / events / date / archived.

## Requirements

- Home Assistant 2024.11 or newer (needed for bundled go2rtc).
- The official [`nest`](https://www.home-assistant.io/integrations/nest/) integration already set up with SDM credentials.
- `ffmpeg` (HA's built-in ffmpeg integration is used automatically).
- An AWS account with an IAM user scoped to a single bucket. Recommended policy:
  - `s3:PutObject`, `s3:GetObject`, `s3:HeadObject`, `s3:ListBucket`, `s3:RestoreObject` on your bucket.

## Installation (HACS)

1. Add this repo as a custom repository in HACS (category: Integration).
2. Install **Nest Recorder**.
3. Restart Home Assistant.
4. Settings → Devices & Services → Add Integration → "Nest Recorder". Enter AWS credentials, bucket, region, and local storage path.
5. Per-camera toggles and retention live in the integration's **Options**.

## Services

| Service | Purpose |
| --- | --- |
| `nest_recorder.record_clip` | Start an out-of-band event clip on a camera (creates a `manual` event row). |
| `nest_recorder.restore_from_glacier` | Initiate a restore for a segment or a camera/time-range. Fires a persistent notification when ready. |
| `nest_recorder.rebuild_index` | Reconcile the SQLite index with what's on disk and in S3. |
| `nest_recorder.purge_local` | Evict local files older than N days (S3 untouched). |
| `nest_recorder.extend_retention` | Pin a segment from GC until a date. |

## Media browser

Recordings appear under the HA Media tab:

- `Nest Recorder › <camera> › events › <day>` — motion / person / doorbell clips.
- `Nest Recorder › <camera> › timeline › YYYY › MM › DD` — all raw 10-minute segments.
- `Nest Recorder › <camera> › archived` — segments with no local copy. Call `restore_from_glacier` on any of them and they move back once ready.

## Limitations

- **Battery Nest cameras cannot do true 24/7.** The SDM API only produces a stream on-demand during motion/viewing. This integration records whatever is available and surfaces daily coverage in the `nest_recorder.status` sensor.
- **SDM RTSP URLs expire every ~5 minutes** and can only be extended ~5 times. Expect a 1–2 second gap at regen.
- **SDM rate limits** (~10 `GenerateRtspStream`/minute/structure). Startup is serialized.
- **Glacier early-delete minimums**: `GLACIER` and `GLACIER_IR` 90 days, `DEEP_ARCHIVE` 180 days. If your local retention is shorter, you'll pay storage for data you already deleted locally.
- **Disk pressure**: at 10-minute segments a 1080p camera is ~30–70 MB/segment → 4–10 GB/day.

## Known risks

- AWS credentials live in HA's config entry storage (same place as other cloud integrations). Rotate via Options.
- S3 DeleteObject is **never** issued by this integration — lifecycle rules are your responsibility.
- The SQLite index lives at `<config>/nest_recorder/recorder.db` — back it up with the rest of your HA config.

## Development

Integration code lives in `custom_components/nest_recorder/`. Validation:

```bash
pip install homeassistant hassfest
python -m script.hassfest --action validate
```

The plan file at `.claude/plans/` describes architecture and verification steps.
