---
name: git-tag-releaser
description: Manages Semantic Versioning (SemVer) git tags and automated releases for URL Shortener using GoReleaser and Docker Hub CI/CD pipelines. Use whenever creating, bumping, verifying, or pushing git release tags (vX.Y.Z).
license: MIT
compatibility: opencode
metadata:
  language: id
  standard: semantic-versioning-v2
---

## What I do

- Standardize Git Tag creation using **Semantic Versioning 2.0.0** (`vMAJOR.MINOR.PATCH`).
- Determine the correct version bump (`v1.0.0` ➔ `v1.0.1` vs `v1.1.0` vs `v2.0.0`) based on recent commit history.
- Create annotated Git release tags with proper release notes.
- Push release tags to GitHub to trigger automated **Release (GoReleaser)** and **CD (Docker Hub SemVer Tagging)** workflows.

## When to use me

Use this skill whenever you need to release a new version of the application, create or push a git tag, or check release versioning status in this repository.

---

## Semantic Versioning Rules (`vMAJOR.MINOR.PATCH`)

Format tag wajib diawali huruf `v` kecil: **`vMAJOR.MINOR.PATCH`**

| Version Component | Trigger Rule | Example Transition |
| :--- | :--- | :--- |
| **MAJOR** (`vX.0.0`) | Terdapat `BREAKING CHANGE:` atau perubahan yang mematahkan backward compatibility pada API. | `v1.4.2` ➔ `v2.0.0` |
| **MINOR** (`v1.X.0`) | Penambahan fitur baru (`feat:`) yang backward-compatible. | `v1.4.2` ➔ `v1.5.0` |
| **PATCH** (`v1.0.X`) | Perbaikan bug (`fix:`) atau peningkatan performa (`perf:`) yang backward-compatible. | `v1.4.2` ➔ `v1.4.3` |
| **Pre-Release** (`-rc.X`) | Release candidate untuk pengujian staging/QA. | `v1.5.0-rc.1` |

---

## Instructions & Step-by-Step Workflow

### Step 1: Check Current Version Status
Sebelum membuat tag baru, periksa daftar tag lokal & remote serta commit rilis terakhir:
```bash
# Tampilkan tag terakhir yang ada
git describe --tags --abbrev=0

# Tampilkan seluruh daftar tag berurutan
git tag -l --sort=-v:refname

# Periksa status working tree (wajib bersih)
git status
```

### Step 2: Determine Next Version
Analisis daftar commit sejak tag rilis terakhir (`git log <last-tag>..HEAD --oneline`):
- Jika ada commit `BREAKING CHANGE:` atau `feat!:` ➔ Naikkan **MAJOR** (`v2.0.0`).
- Jika ada commit `feat:` ➔ Naikkan **MINOR** (`v1.1.0`).
- Jika hanya ada commit `fix:`, `perf:`, `refactor:` ➔ Naikkan **PATCH** (`v1.0.1`).

### Step 3: Create Annotated Git Tag
Selalu buat **Annotated Tag** (bukan lightweight tag) dengan pesan rilis yang jelas:
```bash
# Format: git tag -a vX.Y.Z -m "vX.Y.Z Judul Rilis"
git tag -a v1.0.0 -m "v1.0.0 Initial Production Release"
```

### Step 4: Push Tag to Remote
Push tag ke GitHub untuk memicu pipeline CI/CD rilis otomatis:
```bash
# Push tag spesifik yang baru dibuat
git push origin v1.0.0
```

---

## Automated Pipelines Triggered by Tag Push

Saat tag `vX.Y.Z` di-push ke GitHub, sistem CI/CD akan secara otomatis menjalankan:

1. **GitHub Release & GoReleaser (`release.yml`)**:
   - Membangun biner Go statis untuk `linux/amd64`, `linux/arm64`, `darwin/amd64`, `darwin/arm64`, `windows/amd64`, `windows/arm64`.
   - Meng-embed React SPA frontend (`web/dist`).
   - Meng-generate changelog otomatis dan mempublikasikan berkas arsip rilis (`.tar.gz`, `.zip`, `checksums.txt`) di halaman **GitHub Releases**.

2. **Docker Hub SemVer Image Push (`cd.yml`)**:
   - Mem-build multi-arch Docker container image.
   - Meng-push tag SemVer otomatis ke Docker Hub:
     - `username/repository:X.Y.Z`
     - `username/repository:X.Y`
     - `username/repository:X`
     - `username/repository:latest`

---

## Example Shell Commands Quick Reference

```bash
# 1. Periksa tag saat ini
git describe --tags --abbrev=0

# 2. Lihat log commit sejak tag terakhir
git log $(git describe --tags --abbrev=0)..HEAD --oneline

# 3. Buat tag PATCH rilis baru
git tag -a v1.0.1 -m "v1.0.1 Perbaikan bug pada rate limiting"

# 4. Push tag ke remote
git push origin v1.0.1
```
