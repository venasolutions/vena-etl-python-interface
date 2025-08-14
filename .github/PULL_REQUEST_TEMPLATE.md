#\<proj\>-\<ticket number\>

Use this format to specify the jira ticket which this pull request fixes or delivers. This way the automation tool can properly link the jira ticket when necessary. For cases where there are multiple stories, specify each story in the right format on multiple lines.
e.g.

#XYZ-123

### [pick one] Issue / Feature / Enhancement / Other
Link to the Zendesk ticket / Design Document (if available).
If no link available, please describe why the change is needed and how urgent it is.

### Customer visible change [REQUIRED: pick one]
Yes / No 
*DO NOT DELETE THIS HEADING - ANSWER THE QUESTION! READ ON*
If the PR fixes a customer visible bug, or otherwise changes product behavior in a way customers can experience, answer Yes.
If the PR is part of a quarterly release feature that has not yet shipped, or is not something customers can experience, answer No.
Delete the rest of this section other than the heading and the Yes or No answer.

### Customer visible change details
If you answered No to "Customer visible change", *DELETE this section*. DO NOT answer N/A here.
Otherwise *replace* the contents of this section with the details of the change as the customer will experience it. Those details will be added to the Jira issue when the pull request goes to production. Our technical writers search Jira for those details to document customer visible changes in their biweekly Release Updates to customers.

### Root Cause Analysis
(This section is only needed for bug fixes) What causes this bug to happen? Was it a regression (something that worked originally but then stopped working later)? If yes, what change caused it to break?

### Code change / Fix
Describe what your code does. If this is a bug fix, describe how your change fixes it.

### Risk assessment [REQUIRED]
Does this PR involve:

- If you've checked any of the following add `requires-review:hotfix-process` label:
  - * [ ] A hotfix? => Follow the [hotfix process](https://venasolutions.atlassian.net/wiki/spaces/DEV/pages/820346947/Hotfix+Change+Management+Deployment+Process)
- If you've checked any of the following add `requires-review:cab` label:
  - * [ ] Security: Changes to authentication or authorization code? 
  - * [ ] A new major feature?
  - * [ ] Data integrity (functional): Can't confidently say there's no risk of data corruption and lowered Customer Trust in data?
  - * [ ] Data privacy (GDPR/PIPEDA): Exporting or logging sensitive data or Personally Identifiable Information (PII) to another system outside of Vena, like: Datadog, Sentry and Pendo?
  - * [ ] Any data encryption/decryption changes?
  - * [ ] Making a change to the Risk Assessment questions PR template?
  - * [ ] Changing or removing a guardrail or limit? (eg. editing the PR template, increasing capacity for a containerized service)
- If you've checked any of the following, add `requires-review:api-chapter` label:
  - * [ ] Adding new API endpoint? => Follow the [New API endpoint review process](https://venasolutions.atlassian.net/wiki/spaces/DEV/pages/3646521346/New+API+endpoint+review+process).
  - * [ ] Contract changes for any API that is currently in use? => Follow [Existing API endpoint review process](https://venasolutions.atlassian.net/wiki/spaces/DEV/pages/3661627461/Existing+API+endpoint+review+process)
- If you've checked any of the following add `requires-review:team-architect` label:
  - * [ ] Has potential to cause increased resource usage or decreased performance?
  - * [ ] Creates or modifies an `@Entity` class? Provide the output of `SHOW CREATE TABLE` SQL command for the affected table(s) after the change (it is acceptable to provide just the changed portion).
  - * [ ] Creates or modifies a subclass of ETLStep, ETLJob, or any MongoDB (Morphia) entity? Provide a sample document/field showing the change.
  - * [ ] Creates or modifies an upgrader, or creates/changes/deletes database tables, collections, indexes, or constraints?
  - * [ ] Modifies a bosk `StateTreeNode` or `Entity` (see [Schema evolution: how to add a new field](https://github.com/venasolutions/bosk/blob/develop/docs/USERS.md#schema-evolution-how-to-add-a-new-field))
  - * [ ] Adding/removing any dependency (including for tests)?
  - * [ ] Not sure if this code is maintainable longer term?
- If you've checked the following, add `requires-review:third-party-libraries` label:
  - * [ ] Adding an external runtime dependency? (prerequisite: contact [#third_party_libraries](https://vena.slack.com/archives/C013G16719N))
- If you've checked any of the following add `requires-review:team-sdm` label:
  - * [ ] Disabling a test? => Follow the [policy outline](https://venasolutions.atlassian.net/wiki/spaces/DEV/pages/4134567969/Policy+on+disabling+tests)
- If you haven't checked any of the above please check the following add `requires-review:team-only` label:
  - * [ ] No to all

<!---
Helpful links
- [Change management procedure](https://venasolutions.atlassian.net/wiki/spaces/DEV/pages/3192455173/Change+management+procedure)
- [Normal Lite Model](https://venasolutions.atlassian.net/wiki/spaces/DEV/pages/3192455173/Change+management+procedure#1.-Normal-Lite-Model)
- [Review Boards](https://venasolutions.atlassian.net/wiki/spaces/DEV/pages/3208249345/Risk+Assessment+by+codebase)
--->

### Side effects
List any side effects anticipated (for the end user or for developers).
List any schema changes.
List any API changes.
List any config file changes.

### Tests
- Unit Tests
  - * [ ] Are there unit tests for all new or modified code? Is the coverage sufficient? 
- API Tests
  -  * [ ]​ Are all API endpoints tested, including both success and failure scenarios? 
  - If adding new API endpoint, add:
    - * [ ] API test for basic functionality of new API i.e. query parameter, success code, headers, body etc. 
    - * [ ] API test for permissions
- E2E Tests
  - * [ ] Do E2E tests cover visual elements, interactions, and user experience? 
  - * [ ] List manual tests performed   
- If committing code that could break PowerBI dataset schema or affect refresh (cases are listed under [PowerBI breaking changes](https://venasolutions.atlassian.net/wiki/spaces/DI/pages/3524755461/PowerBI+refresh+checklist#Breaking-changes))
  - * [ ] [PowerBI refresh checklist](https://venasolutions.atlassian.net/wiki/spaces/DI/pages/3524755461/PowerBI+refresh+checklist) has been performed
- General
  - * [ ] Are there any areas as part of PR that are still not tested?
  - * [ ] Follow up Jira tickets created for untested code?
  - * [ ] Other reason, list below.

### Dependencies
List any dependencies that must be resolved before this can be merged. Examples:
- [ ] #XYZ to be merged first

### Shout Outs
Tag someone who helped you get this PR submitted!