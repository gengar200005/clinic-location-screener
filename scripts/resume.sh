#!/usr/bin/env bash
# 환경 이동 시 작업 재개: git pull → venv → .env 확인 → pytest smoke
# 사용: bash scripts/resume.sh
set -e

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

echo "[1/5] git pull (fast-forward only)"
git pull --ff-only

echo "[2/5] venv 확인"
if [ ! -d ".venv" ]; then
  echo "✗ .venv 없음. 먼저 실행: python -m venv .venv"
  exit 1
fi

if [ -f ".venv/Scripts/activate" ]; then
  source .venv/Scripts/activate
elif [ -f ".venv/bin/activate" ]; then
  source .venv/bin/activate
else
  echo "✗ venv activate 스크립트 없음"
  exit 1
fi

echo "[3/5] 의존성 동기화 (requirements.txt)"
pip install -q -r requirements.txt

echo "[4/5] .env 확인"
if [ ! -f ".env" ]; then
  echo "✗ .env 없음. .env.example 복사 후 키 채우세요:"
  echo "  cp .env.example .env"
  exit 1
fi

echo "[5/5] pytest smoke"
pytest tests/ -q

echo ""
echo "✓ 준비 완료. 다음: claude 실행 → /session-start"
