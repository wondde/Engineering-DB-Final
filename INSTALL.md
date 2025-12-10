# 설치 가이드 (Installation Guide)

이 문서는 Windows, macOS, Linux 모든 운영체제에서 프로젝트를 실행하기 위한 상세한 설치 가이드입니다.

## 목차
- [사전 요구사항](#사전-요구사항)
- [설치 단계](#설치-단계)
- [문제 해결](#문제-해결)
- [운영체제별 특이사항](#운영체제별-특이사항)

---

## 사전 요구사항

### Python 버전
- **최소 버전**: Python 3.8
- **권장 버전**: Python 3.9 이상

### Python 설치 확인

```bash
python --version
```

또는

```bash
python3 --version
```

> **주의**: 시스템에 따라 `python` 또는 `python3` 명령어를 사용해야 합니다.

### Python이 설치되어 있지 않은 경우

#### Windows
1. [Python 공식 웹사이트](https://www.python.org/downloads/)에서 다운로드
2. 설치 시 **"Add Python to PATH"** 체크박스 선택 필수

#### macOS
```bash
# Homebrew를 이용한 설치 (권장)
brew install python@3.11
```

#### Linux (Ubuntu/Debian)
```bash
sudo apt update
sudo apt install python3 python3-pip
```

---

## 설치 단계

### 1단계: 프로젝트 다운로드

프로젝트 폴더로 이동:

```bash
cd /path/to/edb_final
```

### 2단계: 패키지 설치

#### 방법 A: requirements.txt 이용 (권장)

```bash
pip install -r requirements.txt
```

또는 (시스템에 따라)

```bash
pip3 install -r requirements.txt
```

#### 방법 B: 수동 설치

```bash
pip install pandas>=2.0.0
pip install numpy>=1.24.0
pip install matplotlib>=3.7.0
pip install seaborn>=0.12.0
pip install scikit-learn>=1.3.0
pip install sqlalchemy>=2.0.0
pip install statsmodels>=0.14.0
pip install openpyxl>=3.1.0
```

### 3단계: 설치 확인

```bash
python -c "import pandas, numpy, sklearn, sqlalchemy, statsmodels; print('✅ 모든 라이브러리가 정상적으로 설치되었습니다!')"
```

성공 시 "✅ 모든 라이브러리가 정상적으로 설치되었습니다!" 메시지가 출력됩니다.

### 4단계: 프로그램 실행

```bash
python main.py --mode all
```

---

## 문제 해결

### 문제 1: "ModuleNotFoundError: No module named 'statsmodels'"

**원인**: statsmodels 패키지가 설치되지 않음

**해결방법**:
```bash
pip install statsmodels>=0.14.0
```

### 문제 2: "WARNING findfont: Font family 'NanumGothic' not found"

**원인**: 한글 폰트 경고 (실행에는 영향 없음)

**해결방법**:
- **Windows**: 이미 해결됨 (Malgun Gothic 자동 사용)
- **macOS**: 이미 해결됨 (AppleGothic 자동 사용)
- **Linux**: 선택사항 - 한글 폰트 설치
  ```bash
  sudo apt-get install fonts-nanum
  ```

### 문제 3: "pip: command not found"

**원인**: pip가 설치되지 않음

**해결방법**:
```bash
# Linux/macOS
python3 -m ensurepip --upgrade

# Windows
python -m ensurepip --upgrade
```

### 문제 4: 권한 오류 (Permission Denied)

**해결방법**:
```bash
# 가상환경 사용 (권장)
python -m venv venv
source venv/bin/activate  # Linux/macOS
venv\Scripts\activate     # Windows

# 이후 다시 설치
pip install -r requirements.txt
```

또는

```bash
# 사용자 디렉토리에 설치
pip install --user -r requirements.txt
```

### 문제 5: SSL 인증서 오류

**해결방법**:
```bash
pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org -r requirements.txt
```

---

## 운영체제별 특이사항

### Windows

#### 인코딩 문제 해결
- 프로젝트 파일은 모두 UTF-8로 인코딩되어 있음
- 최신 Windows 10/11에서는 문제없이 동작

#### 명령 프롬프트 설정
```cmd
# PowerShell 사용 권장
chcp 65001  # UTF-8 인코딩 설정
```

#### 한글 폰트
- **기본 폰트**: Malgun Gothic
- **추가 설치 불필요**

### macOS

#### Homebrew를 통한 Python 설치 권장
```bash
brew install python@3.11
```

#### 한글 폰트
- **기본 폰트**: AppleGothic
- **추가 설치 불필요**

### Linux (Ubuntu/Debian)

#### Python 및 pip 설치
```bash
sudo apt update
sudo apt install python3 python3-pip python3-venv
```

#### 한글 폰트 설치 (선택사항)
```bash
sudo apt-get install fonts-nanum fonts-nanum-coding
fc-cache -fv
```

#### 한글 폰트
- **기본 동작**: sans-serif (영문 폰트)
- **선택사항**: NanumGothic 설치 가능
- **폰트 없이도 정상 실행됨**

---

## 가상환경 사용 (권장)

프로젝트별로 독립적인 패키지 환경을 유지하려면 가상환경 사용을 권장합니다.

### 가상환경 생성 및 활성화

#### Windows
```cmd
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

#### macOS/Linux
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 가상환경 비활성화
```bash
deactivate
```

---

## 설치 후 실행 방법

### 전체 파이프라인 실행
```bash
python main.py --mode all
```

### 단계별 실행
```bash
# 1. 데이터 정제만
python main.py --mode etl

# 2. 데이터베이스 저장만
python main.py --mode load

# 3. SQL 분석만
python main.py --mode analyze

# 4. 머신러닝 모델만
python main.py --mode ml
```

---

## 추가 정보

### 패키지 버전 확인
```bash
pip list | grep -E "pandas|numpy|sklearn|statsmodels"
```

### 패키지 업그레이드
```bash
pip install --upgrade -r requirements.txt
```

### 문제가 해결되지 않을 경우
1. 가상환경을 새로 만들어 시도
2. Python 버전 확인 (3.8 이상 필요)
3. pip 업그레이드: `pip install --upgrade pip`
4. 캐시 삭제 후 재설치: `pip install --no-cache-dir -r requirements.txt`

