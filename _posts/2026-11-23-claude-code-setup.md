---
published: false
title: "새 컴퓨터에 Claude Code 환경 그대로 옮기기"
date: 2026-11-23 21:00:00 +0900
categories: [AI & Tooling, Claude Code]
tags: [claude-code, plugin, skill, mcp, setup]
description: "Claude Code에 설치해 쓰는 플러그인 14개, 스킬 32개, MCP 서버 6개를 새 컴퓨터에서 순서대로 다시 설치하는 명령을 정리한다."
---

Claude Code에 플러그인, 스킬, MCP 서버가 꽤 쌓였다. 다른 컴퓨터에도 같은 환경을 만들려고 지금 설치된 것을 전부 뽑아 설치 순서대로 정리했다. 2026년 11월 23일, macOS 기준이다.

설치는 다섯 단계다.

1. 기본 도구
2. 플러그인
3. 스킬
4. MCP 서버
5. 설정 파일 복사

## 1. 기본 도구

플러그인과 MCP 서버가 실행 중에 부르는 프로그램이다. 먼저 깔아 둔다.

```bash
brew install node python ffmpeg yt-dlp
brew install oven-sh/bun/bun
curl -LsSf https://astral.sh/uv/install.sh | sh
```

| 도구 | 쓰는 곳 |
|---|---|
| `node` | `npx`로 띄우는 MCP 서버, Skills CLI |
| `bun` | oh-my-claudecode의 패키지 설치 |
| `python` | watch 플러그인의 스크립트 |
| `ffmpeg`, `yt-dlp` | watch가 영상을 받고 프레임을 뽑을 때 |
| `uv` | Python으로 만든 CLI 설치 |

`bun`은 빠뜨리기 쉽다. 없어도 플러그인 설치는 성공하고 상태도 `enabled`로 나온다. 다만 `claude plugin list`에서 oh-my-claudecode 아래에 "bun이 없어 패키지를 설치하지 못했다"는 메모가 붙는다. 지금 쓰는 컴퓨터가 그 상태였다.

## 2. 플러그인

플러그인은 마켓플레이스를 먼저 등록하고 거기서 골라 설치한다. 마켓플레이스 등록만으로는 아무것도 설치되지 않는다.

```bash
claude plugin marketplace add https://github.com/Yeachan-Heo/oh-my-claudecode.git
claude plugin marketplace add https://github.com/affaan-m/ECC.git
claude plugin marketplace add anthropics/claude-plugins-official
claude plugin marketplace add anthropics/skills
claude plugin marketplace add team-attention/plugins-for-claude-natives
claude plugin marketplace add heygen-com/hyperframes
claude plugin marketplace add mvanhorn/last30days-skill
claude plugin marketplace add google/elephant-goldfish-model
claude plugin marketplace add 21st-dev/magic-mcp
claude plugin marketplace add bradautomates/claude-video
```

```bash
claude plugin install oh-my-claudecode@omc
claude plugin install ecc@ecc
claude plugin install code-review@claude-plugins-official
claude plugin install commit-commands@claude-plugins-official
claude plugin install frontend-design@claude-plugins-official
claude plugin install security-guidance@claude-plugins-official
claude plugin install kotlin-lsp@claude-plugins-official
claude plugin install document-skills@anthropic-agent-skills
claude plugin install kakaotalk@team-attention-plugins
claude plugin install hyperframes@hyperframes
claude plugin install last30days@last30days-skill
claude plugin install egm@egm
claude plugin install 21st@21st-dev
claude plugin install watch@claude-video
```

| 플러그인 | 하는 일 |
|---|---|
| [oh-my-claudecode](https://github.com/Yeachan-Heo/oh-my-claudecode) | 여러 에이전트를 역할별로 나눠 돌리는 오케스트레이션, 상태줄 HUD |
| [ecc](https://github.com/affaan-m/ECC) | 에이전트 72개, 스킬 328개 묶음. 언어별 리뷰어와 빌드 오류 해결 에이전트 |
| code-review | 여러 에이전트가 PR을 리뷰하고 확신도로 걸러 낸다 |
| commit-commands | 커밋, 푸시, PR 생성 명령 |
| frontend-design | 프런트엔드 화면을 만들 때의 디자인 지침 |
| security-guidance | Claude가 쓴 코드의 보안 점검 |
| kotlin-lsp | Kotlin 언어 서버 |
| document-skills | Excel, Word, PowerPoint, PDF 파일 처리 |
| kakaotalk | macOS에서 카카오톡 메시지 보내고 읽기 |
| [hyperframes](https://github.com/heygen-com/hyperframes) | HTML을 써서 영상으로 렌더링 |
| [last30days](https://github.com/mvanhorn/last30days-skill) | Reddit, X, YouTube, Hacker News 등에서 최근 30일 글을 모아 조사 |
| [egm](https://github.com/google/elephant-goldfish-model) | 문서를 처음 보는 눈으로 리뷰 |
| 21st | UI 컴포넌트를 찾아 코드를 가져온다 |
| [watch](https://github.com/bradautomates/claude-video) | 영상 URL이나 파일을 프레임과 자막으로 바꿔 Claude에게 넘긴다 |

설치가 끝나면 확인한다.

```bash
claude plugin list
```

14개가 모두 `enabled`면 된다. 플러그인 아래에 `Note:` 줄이 붙어 있으면 읽어 본다. 의존 패키지가 빠졌다는 안내가 거기에 나온다.

oh-my-claudecode는 설치 뒤에 Claude Code 안에서 `/oh-my-claudecode:omc-setup`을 한 번 돌린다. 이 명령은 `~/.claude/CLAUDE.md`에서 `<!-- OMC:START -->`와 `<!-- OMC:END -->` 사이만 관리한다. 그 바깥에 직접 쓴 지침은 그대로 둔다.

## 3. 스킬

플러그인에 묶이지 않은 스킬은 [Skills CLI](https://github.com/vercel-labs/skills)로 받는다. `-g`를 붙이면 프로젝트가 아니라 사용자 전역에 설치된다.

```bash
# obra/superpowers: 계획, TDD, 디버깅, 코드 리뷰 절차 14개
for s in brainstorming dispatching-parallel-agents executing-plans \
  finishing-a-development-branch receiving-code-review requesting-code-review \
  subagent-driven-development systematic-debugging test-driven-development \
  using-git-worktrees using-superpowers verification-before-completion \
  writing-plans writing-skills; do
  npx skills add obra/superpowers -g --skill "$s"
done

# team-attention/plugins-for-claude-natives: 14개
for s in agent-council metamedium unknown vague review youtube-digest \
  google-calendar history-insight session-analyzer session-wrap dev-scan \
  tech-decision kakaotalk gmail; do
  npx skills add team-attention/plugins-for-claude-natives -g --skill "$s"
done

npx skills add vercel-labs/skills -g --skill find-skills
npx skills add blader/humanizer -g --skill humanizer
npx skills add nextlevelbuilder/ui-ux-pro-max-skill -g --skill ui-ux-pro-max
npx skills add anthropics/skills -g --skill skill-creator
```

| 출처 | 스킬 | 용도 |
|---|---|---|
| [obra/superpowers](https://github.com/obra/superpowers) | 14개 | 계획 작성, TDD, 체계적 디버깅, 리뷰 주고받기 |
| [team-attention](https://github.com/team-attention/plugins-for-claude-natives) | 14개 | 모호한 요구사항 다듬기, 세션 기록 분석, Gmail과 캘린더 |
| vercel-labs/skills | find-skills | 필요한 스킬 검색 |
| blader/humanizer | humanizer | AI가 쓴 티가 나는 문장 고치기 |
| nextlevelbuilder | ui-ux-pro-max | UI 스타일, 색, 글꼴 조합 참고 자료 |
| anthropics/skills | skill-creator | 스킬 만들기와 평가 |

어떤 스킬을 어디서 받았는지는 `~/.agents/.skill-lock.json`에 남는다. 이 글의 목록도 거기서 뽑았다.

```bash
jq -r '.skills | to_entries[] | "\(.key)\t\(.value.source)"' ~/.agents/.skill-lock.json
```

## 4. MCP 서버

자체 CLI가 필요한 서버가 있어서 그것부터 깐다.

```bash
npm install -g agent-desktop
uv tool install scrapling
```

```bash
claude mcp add -s user context7 -- npx -y @upstash/context7-mcp
claude mcp add -s user playwright -- npx @playwright/mcp@latest
claude mcp add -s user filesystem -- npx -y @modelcontextprotocol/server-filesystem <허용할 디렉터리> /tmp
claude mcp add -s user agent-desktop -- agent-desktop mcp
claude mcp add -s user scrapling -- scrapling mcp
claude mcp add -s user craft-anything -- craft-anything-cli mcp
```

| 서버 | 하는 일 |
|---|---|
| context7 | 라이브러리의 최신 문서를 가져온다 |
| playwright | 브라우저를 열어 클릭하고 스크린샷을 찍는다 |
| filesystem | 지정한 디렉터리의 파일 읽고 쓰기 |
| agent-desktop | macOS 앱을 접근성 트리로 읽고 조작 |
| scrapling | 웹 페이지 수집 |
| craft-anything | 배포된 프로그램이 어떻게 동작하는지 조사 |

`filesystem`에 넘기는 경로는 서버가 접근할 수 있는 범위다. 홈 디렉터리 전체가 아니라 필요한 폴더만 준다.

`craft-anything`은 `craft-anything-cli` 실행 파일이 따로 있어야 한다.

Gmail, Google Drive, Google Calendar, Notion, Slack 커넥터는 claude.ai 계정에 묶여 있다. 새 컴퓨터에서 로그인하면 따로 설치하지 않아도 보인다. 인증이 풀린 것만 다시 연결하면 된다.

```bash
claude mcp list
```

모든 줄이 `Connected`면 끝이다.

## 5. 설정 파일 복사

명령으로 다시 받을 수 없는 파일은 직접 복사한다.

- `~/.claude/settings.json`: 권한, hook, 상태줄 설정
- `~/.claude/CLAUDE.md`: 전역 지침
- `~/.claude/commands/`: 직접 만든 슬래시 명령

복사하기 전에 두 파일이 무엇에 기대는지 확인한다. 설정만 옮기고 프로그램이 없으면 hook이 실행될 때마다 오류가 난다.

```bash
# settings.json의 hook이 부르는 명령
jq -r '.hooks | to_entries[] | .key as $e | .value[].hooks[]?.command | "\($e)\t\(.[0:80])"' ~/.claude/settings.json

# CLAUDE.md가 끌어오는 다른 파일
grep -n '^@' ~/.claude/CLAUDE.md
```

내 설정에서는 이렇게 나왔다.

| 설정 | 기대는 것 |
|---|---|
| `PreToolUse` hook | `rtk` 실행 파일 |
| `Stop` hook | `~/.claude/count_tokens.js` |
| `CLAUDE.md`의 `@RTK.md` | `~/.claude/RTK.md` |
| `CLAUDE.md`의 graphify 항목 | `graphify` CLI와 스킬 |

`graphify`는 `uv`로 설치한다. 패키지 이름은 `y`가 두 개다.

```bash
uv tool install graphifyy
```

`settings.json`에는 절대 경로가 들어 있는 hook도 있다. 사용자 이름이 다른 컴퓨터로 옮기면 경로를 고쳐야 한다.

## English summary

This post lists everything I have installed on top of Claude Code and the commands to reinstall it on another Mac: 14 plugins from 10 marketplaces, 32 skills installed with the Skills CLI, and 6 user-scope MCP servers. Install the base tools first, because a missing `bun` lets plugin installation succeed while skipping the plugin's packages. The source of every skill is recorded in `~/.agents/.skill-lock.json`, which is where the skill list came from. Settings, global instructions and custom slash commands are plain files to copy by hand, and the hooks in `settings.json` depend on programs that must be installed first.
