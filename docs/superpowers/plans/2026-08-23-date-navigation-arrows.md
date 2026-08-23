# Previous/Next Date Navigation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add accessible previous-day and next-day buttons around the existing date field so the map can be browsed one calendar day at a time.

**Architecture:** Keep the existing visible `DD/MM/YYYY` text input, hidden ISO input, `js-datepicker` calendar, and `/donations?donation_date=YYYY-MM-DD` endpoint. Add a small date-navigation group to the existing form and route button clicks through one date-selection helper that synchronizes both inputs, the datepicker selection, and `refresh()`.

**Tech Stack:** Static HTML/CSS/vanilla JavaScript, `qodesmith/js-datepicker`, FastAPI endpoint already used by the frontend.

**Spec:** Bounded request from the conversation; no separate design document is required.

## Global Constraints

- Modify only `frontend/index.html`.
- Do not change the backend endpoint, database, API format, or external dependencies.
- Preserve the current `DD/MM/YYYY` display format, ISO query format, calendar behavior, manual input behavior, language switcher, and mobile calendar positioning.
- Buttons must be real `<button type="button">` elements with localized `aria-label` values.
- Keep the visual order as previous (`‹`) on the left and next (`›`) on the right in all languages; use a local `direction: ltr` wrapper so Hebrew page direction does not reverse the controls.
- `picker.setDate()` does not invoke the library's `onSelect` callback, so button handling must explicitly update the ISO value and call `refresh()`.

---

### Task 1: Add date-navigation controls and a synchronized date helper

**Files:**
- Modify: `frontend/index.html:37-38` for the navigation layout styles.
- Modify: `frontend/index.html:167-173` for the two buttons around the existing date input.
- Modify: `frontend/index.html:185-209` for localized button labels.
- Modify: `frontend/index.html:290-383` for the shared date-selection and click behavior.

**Interfaces:**
- Consumes: `dateUiInp`, `dateIsoInp`, `picker`, `ddmmyyyy()`, `isoFromDate()`, and `refresh()` already defined in `frontend/index.html`.
- Produces: `selectDate(date, syncPicker)` and two controls with IDs `previous-date` and `next-date`.

- [ ] **Step 1: Add the failing behavior check before implementation**

  Start the existing app and record the current date shown in the field. Click the future controls only after implementation; before implementation, verify that `#previous-date` and `#next-date` do not exist. This confirms the change is isolated to the intended UI.

  Run from the repository root:

  ```powershell
  .\.venv\Scripts\python.exe -m uvicorn backend.app:app --reload
  ```

  Open `http://127.0.0.1:8000/` in a browser.

- [ ] **Step 2: Add the button markup and localized labels**

  Replace the single date input area with a navigation wrapper containing:

  ```html
  <div id="date-navigation">
    <button id="previous-date" type="button" aria-label="Previous day">‹</button>
    <input id="date_ui" type="text" size="10" autocomplete="off" inputmode="numeric" placeholder="DD/MM/YYYY">
    <button id="next-date" type="button" aria-label="Next day">›</button>
  </div>
  ```

  Add `previous_date` and `next_date` strings to all three `i18n` entries. Add an attribute translation pass in `applyLang()` so those strings are assigned to the buttons' `aria-label` attributes when the language changes.

- [ ] **Step 3: Style the controls without changing the existing calendar layout**

  Style `#date-navigation` as an inline flex group with `direction: ltr`, centered alignment, and a small gap. Give both buttons a stable touch target, visible focus indication, and no browser form submission behavior. Keep the existing mobile `#date_ui` width rule and verify that the new group does not create horizontal scrolling; allow the date form to wrap on narrow screens if necessary.

- [ ] **Step 4: Add one helper for synchronized date selection**

  After `picker` is initialized, add a helper with this behavior:

  ```js
  function selectDate(date, syncPicker = false) {
    if (syncPicker) picker.setDate(date, true);
    dateUiInp.value = ddmmyyyy(date);
    dateIsoInp.value = isoFromDate(date);
    refresh();
  }
  ```

  Keep the existing `onSelect` callback behavior or route it through the helper without calling `picker.setDate()` again. For manual input, convert the parsed ISO value into a local `Date` and use the same helper so the visible field, hidden field, calendar selection, and map remain synchronized.

- [ ] **Step 5: Implement previous/next day calculation**

  Read the current date from `dateIsoInp.value`, construct a local `Date`, call `setDate(current.getDate() - 1)` or `setDate(current.getDate() + 1)`, and pass the result to `selectDate(nextDate, true)`. Register both listeners with `preventDefault()` so the form never submits or reloads the page.

- [ ] **Step 6: Run the focused browser smoke test**

  Verify all of the following:

  - Initial date and map results are unchanged.
  - Previous changes both displayed and hidden values by exactly one day.
  - Next restores the original date.
  - `31/12/YYYY` → next becomes `01/01/YYYY+1`.
  - `01/01/YYYY` → previous becomes `31/12/YYYY-1`.
  - A datepicker selection still updates the map.
  - Manual `DD/MM/YYYY` entry still updates the map and the calendar highlight.
  - Empty result days are still selectable and display an empty map without errors.
  - Hebrew, English, and Russian labels expose the correct localized `aria-label` values.
  - On a narrow/mobile viewport, both buttons remain tappable and no horizontal scrollbar appears.

- [ ] **Step 7: Inspect the final diff and repository state**

  Run:

  ```powershell
  git -c safe.directory='C:/Users/User/AnacondaProjects/BloodDonation' diff -- frontend/index.html
  git -c safe.directory='C:/Users/User/AnacondaProjects/BloodDonation' status --short
  ```

  Confirm that only the planned frontend file changed and that no backend or dependency files were modified.
