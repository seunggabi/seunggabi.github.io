---
published: false
title: "GitHub Action 세 개에 기능을 넣고 버그를 고쳤다"
date: 2026-11-20 03:50:00 +0900
categories: [Platform & Infra, GitHub Actions]
tags: [open-source, github-actions, automation, javascript]
---

GitHub 저장소 운영을 자동화하는 작은 Action들이 있다. PR 작성자를 담당자로 지정하고, 리뷰어를 배정하고, 주간 마일스톤을 미리 만들어 두는 것들이다. 이런 Action 세 개에 PR을 보냈고, 모두 머지됐다.

| Action | PR | 바꾼 것 |
|---|---|---|
| readmeio/scheduled-milestones | [#2](https://github.com/readmeio/scheduled-milestones/pull/2), [#4](https://github.com/readmeio/scheduled-milestones/pull/4) | 몇 주 뒤까지 미리 만들기, 제목 날짜 형식, 마감 시각 |
| hkusu/review-assign-action | [#31](https://github.com/hkusu/review-assign-action/pull/31) | GitHub Enterprise에서도 동작 |
| toshimaru/auto-author-assign | [#79](https://github.com/toshimaru/auto-author-assign/pull/79) | PR뿐 아니라 이슈에도 작성자 지정 |

## 주간 마일스톤을 몇 주치 미리 (scheduled-milestones)

`scheduled-milestones`는 정해진 요일로 마일스톤을 만들어 주는 Action이다. 원래는 바로 다음 주 것만 만들었다. 몇 주치를 미리 두려면 매번 손으로 추가해야 했다.

`count`와 `format` 입력을 추가했다. 2022년 7월 22일에 이렇게 설정하면,

```yaml
title: d-
days: Friday
count: 4
format: YYYY.MM.DD
```

마일스톤 네 개가 한 번에 생긴다.

```text
d-2022.07.29
d-2022.08.05
d-2022.08.12
d-2022.08.19
```

이미 있는 제목이면 GitHub API가 `already_exists`로 거절하는데, 원래 코드도 이 경우는 오류로 보지 않고 넘어가게 돼 있어서 매일 돌려도 중복이 생기지 않는다.

메인테이너는 자기들은 이제 이 Action을 쓰지 않으니 동작은 나를 믿겠다며 머지했다.

두 번째 PR(#4)은 마감 시각을 고쳤다. 마일스톤 제목이 `weekly (~ 2022-09-18)`이면 그날 23시 59분 59초까지가 기한이어야 하는데, 만들어진 마일스톤의 `due_on`은 그날 0시였다. 메인테이너가 무슨 사정이냐고 물어서 이 예와 GitHub 커뮤니티 논의 링크, 화면 캡처를 붙여 답했고, 2주 남짓 뒤 머지됐다.

## GitHub Enterprise에서 안 되던 리뷰어 배정 (review-assign-action)

`review-assign-action`은 PR에 담당자와 리뷰어를 자동으로 배정한다. GitHub Enterprise에서는 동작하지 않았다. API 주소를 `api.github.com`으로 고정해서 부르고 있었기 때문이다. Actions 러너가 넣어 주는 `GITHUB_API_URL` 환경 변수를 쓰도록 바꿨다.

```js
// 변경 전
const API_BASE_URL = 'https://api.github.com/repos';
// 변경 후
const API_BASE_URL = process.env.GITHUB_API_URL + '/repos';
```

GitHub Enterprise Server에서 돌면 이 값이 회사 GitHub의 API 주소가 된다. 메인테이너가 머지하고 v1 브랜치로 릴리스하겠다고 남겼다.

## 이슈에도 작성자 지정 (auto-author-assign)

PR을 연 사람을 담당자로 지정하는 Action에 이슈 지원을 넣은 PR이다. 자세한 내용은 작은 PR 모음에 적었다.

## English summary

I sent changes to three small GitHub Actions that automate repository chores. For scheduled-milestones, I added `count` and `format` inputs to create several weekly milestones ahead, and set the due time to the end of the day instead of midnight. For review-assign-action, I made it use the API URL that Actions provides so it works on GitHub Enterprise. For auto-author-assign, I added support for issues. All of them were merged.
