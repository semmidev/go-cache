---
name: git-commit-formatter-id
description: Formats git commit messages using the Conventional Commits spec with an Indonesian-language description. Use whenever writing, reviewing, or fixing a git commit message for this project.
license: MIT
compatibility: opencode
metadata:
  language: id
  standard: conventional-commits
---

## What I do

- Format commit messages strictly as `<type>(<scope>): <description>` according to Conventional Commits standard v1.0.0.
- Keep the `type` tag in English (`feat`, `fix`, `docs`, `style`, `refactor`, `perf`, `test`, `build`, `ci`, `chore`, `revert`) — never translated.
- Write the `<description>` in active, formal Bahasa Indonesia (kalimat aktif baku dengan imbuhan yang sesuai).
- Support multi-line commit body and `BREAKING CHANGE:` footers.
- Ensure alignment with GoReleaser changelog auto-generation and Semantic Versioning (SemVer).

## When to use me

Use this skill whenever creating, writing, reviewing, or fixing git commit messages in this repository.

---

## Commit Structure

```text
<type>(<optional scope>): <description>

[optional body]

[optional footer(s)]
```

---

## Allowed Types (do not translate the type tags)

| Type | Intent / Usage | SemVer Impact |
| :--- | :--- | :--- |
| **feat** | Fitur baru untuk pengguna (a new feature) | MINOR (`v1.X.0`) |
| **fix** | Perbaikan bug / error (a bug fix) | PATCH (`v1.0.X`) |
| **docs** | Perubahan dokumentasi saja (documentation only) | None |
| **style** | Format kode yang tidak mengubah logika (spasi, titik koma, indentasi) | None |
| **refactor** | Restrukturisasi kode tanpa mengubah perilaku eksternal | None |
| **perf** | Peningkatan performa dan efisiensi eksekusi | PATCH (`v1.0.X`) |
| **test** | Penambahan atau perbaikan unit test / integration test | None |
| **build** | Perubahan pada sistem build, kompilasi, atau dependensi | None |
| **ci** | Perubahan pada CI/CD workflow (`.github/workflows/`) | None |
| **chore** | Pemeliharaan rutin, pembersihan berkas, atau tugas maintenance | None |
| **revert** | Membatalkan (revert) commit sebelumnya | None |

---

## Recommended Scopes for This Project

- **server**: Perubahan pada backend Go API (`server/`)
- **web**: Perubahan pada frontend React SPA (`web/`)
- **auth**: Autentikasi, JWT, OAuth, lockout, RBAC
- **url**: Manajer URL pendek, QR code, analytics
- **db**: Skema database, SQLC, migrasi
- **docker**: Dockerfile, compose, `.dockerignore`
- **config**: Konfigurasi Viper, `.env`
- **deps**: Dependensi `go.mod` atau `package.json`
- **docs**: Berkas ADR, README, PLAN.md

---

## Rules & Formatting Directives

1. **Type & Scope**:
   - Tentukan `type` yang paling tepat berdasarkan diff perubahan.
   - Gunakan `scope` spesifik dalam tanda kurung jika perubahan terisolasi pada modul tertentu (misal: `feat(auth)`, `fix(url)`).

2. **Description (Bahasa Indonesia Baku)**:
   - Tulis deskripsi menggunakan Bahasa Indonesia yang aktif dan baku (kalimat aktif dengan awalan `me-` atau kata kerja aksi).
   - Contoh kata kerja baku: `menambahkan`, `memperbaiki`, `memperbarui`, `menghapus`, `mengoptimalkan`, `merekstrukturisasi`.
   - Huruf pertama setelah tanda titik dua `:` menggunakan huruf kecil.
   - Jangan akhiri baris judul commit dengan tanda titik.
   - Batasi panjang baris judul commit maksimal 72 karakter.

3. **Body (Opsional)**:
   - Gunakan body jika memerlukan penjelasan tambahan mengenai alasan latar belakang (*why*) dan konsekuensi perubahan.

4. **Breaking Changes**:
   - Jika ada perubahan yang merusak kompatibilitas (breaking change), tambahkan tanda seru `!` setelah type/scope (misal: `feat(api)!: mengubah format response JSON`) ATAU tambahkan footer `BREAKING CHANGE: <penjelasan>` di bagian paling bawah.
   - Berpengaruh pada kenaikan MAJOR version (`vX.0.0`).

---

## Examples

**Good Examples:**
```text
feat(auth): menambahkan fitur login dengan OAuth Google
fix(url): memperbaiki error panic saat statistik klik bernilai null
docs(adr): menambahkan ADR infra-0002 untuk strategi CD berbasis tag
ci(cd): mengonfigurasi build image Docker Hub otomatis saat release
refactor(db): mengoptimalkan kueri pencarian statistik analytics
```

**Breaking Change Example:**
```text
feat(api)!: mengubah format response JSON untuk endpoint v1/urls

BREAKING CHANGE: Struktur response JSON untuk endpoint /v1/urls diubah dari array ke object pagination.
```

**Bad Examples (Avoid These):**
```text
feat: tambah fitur login       # Jangan gunakan kalimat pasif/singkat tak baku
fix: benerin bug               # Jangan gunakan bahasa gaul / slang
fitur(auth): login google      # Type tag wajib dalam Bahasa Inggris (feat, bukan fitur)
```
