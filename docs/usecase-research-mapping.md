# Use Case Research Mapping

Companion to the cyberattack-reconstruction use case. Same structure: end user, real-world
scenario, example query, business decision, related research with an explicit "what is missing"
column, and datasets with licensing and ground-truth strategy.

Trimmed to the **three most relevant papers and three most relevant datasets per use case**. Every
paper was verified against its arXiv abstract page, ACL Anthology or journal record; every dataset
link was checked for public availability and licence.

---

## 1. Extraction and QA from Large-Scale Temporal Documents

**End user:** Any user who must answer "what was true, and what did we know, at a past date" over a
decade of accumulated documents.

**Real-world use case:** A bank gets a government request about a loan rule used in 2023. The
information is spread across 10 years of letters, internal files, meeting notes, yearly reports and
public statements — about 1 GB of text. Rules often replace, change, or go back in time. The system
reads everything, pulls out facts with dates and sources, and shows the exact rule on any past date
along with which file set it and which file ended it.

**Example query:**
> What was a specific exposure limit that applied to us on 15 March 2023, which circular set it,
> when was it superseded, and did we know about the change at the time?

**Business decision:** Scope the audit response, decide whether a restatement or voluntary
disclosure is required, identify which transactions fall inside the affected window, and produce a
citation-backed timeline for the regulator.

### Relevant research papers

| Research | What it demonstrates | Scale | What is missing |
|---|---|---|---|
| **ATOM** — Lairgi, Moncla, Benabdeslem, Cazabet & Cléau, *AdapTive and OptiMized dynamic temporal knowledge graph construction using LLMs*, **Findings of EACL 2026**, arXiv:2510.22590 | Splits documents into minimal self-contained "atomic facts", builds atomic TKGs with **dual-time modelling that distinguishes when information is observed from when it is valid**, then merges them in parallel. **+18% extraction exhaustivity, +33% stability across runs, >90% latency reduction** vs baselines | Few-shot and designed for scalability; corpus sizes not reported at GB scale | **Closest competitor on bi-temporal extraction.** Parallel merge is intra-process, not a distributed graph engine. **No retrieval or QA layer** — it builds the graph, it never answers a point-in-time question over it. No index-cost reporting at 1 GB. |
| **T-GRAG** — *A Dynamic GraphRAG Framework for Resolving Temporal Conflicts and Redundancy in Knowledge Retrieval*, arXiv:2508.01680 (Aug 2025) | End-to-end temporal GraphRAG: time-stamped KG generator, temporal query decomposition, three-layer interactive retriever (temporal subgraph → candidate nodes → fine-grained facts). Introduces the **Time-LongQA** benchmark from real corporate annual reports | Corporate annual reports; corpus size not reported, single machine | **Closest competitor at system level.** Single timeline only — structurally cannot answer "did we know about the change at the time". No distributed execution, no index- or update-cost reporting, no confidence or provenance carried on extracted facts. |
| **VersionRAG** — Huwiler, Stockinger & Fürst, arXiv:2510.08109 (Oct 2025) | Hierarchical graph modelling **document evolution**, intent-based query routing for version-aware filtering and change tracking. **90% accuracy** vs naive RAG 58% and GraphRAG 64%; **60% on implicit change detection** where baselines score 0–10%; **97% fewer indexing tokens than GraphRAG** | **VersionQA:** 100 curated questions over 34 versioned technical documents | Targets "which file set it and which file ended it" precisely — but versions **documents**, not facts, so there are no validity intervals on `(s, r, o)`. No transaction time. No distributed execution. Benchmark is 34 documents. |

*Also considered, cut for brevity: GraphRAG (arXiv:2404.16130) and LightRAG (arXiv:2410.05779) as
non-temporal baselines; HippoRAG (NeurIPS 2024); Zep/Graphiti (arXiv:2501.13956) for bi-temporal
agent memory; Towards Practical GraphRAG (arXiv:2507.03226) and LinearRAG (arXiv:2510.10114) for
extraction-cost mitigation; DGRAG (arXiv:2505.19847) for distributed graph RAG.*

### The gap, stated plainly

ATOM does dual-time extraction but stops at graph construction. T-GRAG does end-to-end temporal
GraphRAG but on a single timeline. VersionRAG answers supersession questions but versions documents
rather than facts, over 34 of them. **Nobody has joined bi-temporal extraction to bi-temporal
retrieval at GB scale on a distributed engine, and nobody has published a benchmark that separates
"what was true then" from "what did we know then."**

### The scale gap

GraphRAG's own evaluation is ~1M tokens (**≈5 MB**). **GraphRAG-Bench** (ICLR 2026), the most
serious GraphRAG benchmark, is 7M words from 20 textbooks — **≈40 MB**. VersionQA is 34 documents.
A **1 GB target is ~25× GraphRAG-Bench and ~200× the GraphRAG paper's own evaluation.** No temporal
GraphRAG system has published index cost, index time or retrieval latency at that size.

### Datasets

| # | Resource | What it gives you | Size | Access & licence |
|---|---|---|---|---|
| 1 | **ChroniclingAmericaQA** — Piryani, Mozafari & Jatowt, **SIGIR 2024**, arXiv:2403.17859 | **Primary corpus and accuracy story.** A large, noisy, temporally ordered corpus *with a released QA set over it*. Three modalities — raw OCR, corrected text, scanned images — so OCR noise becomes a controlled variable rather than a confound | **487K QA pairs**, 1800–1920 | [github.com/DataScienceUIBK/ChroniclingAmericaQA](https://github.com/DataScienceUIBK/ChroniclingAmericaQA) · underlying corpus is Library of Congress **public domain**, bulk-downloadable and far larger than 1 GB, so sliceable to exactly the target size |
| 2 | **Wikipedia full revision history** | **Bi-temporal ground truth.** The edit log gives transaction time (when an assertion entered the record); the article text gives valid time (when the fact was true). Nothing else supplies both axes for free | **31 TB uncompressed** (Jun 2025); 2019 dump 937 GB bz2 / 157 GB 7z. Sliceable to exactly 1 GB | [Wikimedia `pages-meta-history` dumps](https://en.wikipedia.org/wiki/Wikipedia:Database_download) · CC BY-SA |
| 3 | **FiscalQA Pro** — released with *Temporal Misgrounding in Legal RAG*, **ICML 2026 Workshop on AI for Law**, arXiv:2608.09393 | **Closest published analogue to your scenario, and your external baseline.** A versioned regulatory corpus with expert-reviewed point-in-time questions. The paper names and quantifies **temporal misgrounding** — retrieving the currently in-force version when an earlier or later one applies | **32,436 article-versions** of the French tax code across 93 years (1938–2031); **209 scored, expert-reviewed questions** over 33 articles | Released with the paper; repo linked from the arXiv page |

*Also considered, cut for brevity: ComplexTempQA (100M+ QA, EMNLP 2024); MultiTQ (~500K QA, ACL
2023) for TKGQA comparability; HoH (arXiv:2503.04800) for outdated-information harm; VersionQA;
GraphRAG-Bench. ArchivalQA (532K QA) was excluded outright — it requires the paid **LDC2008T19**
licence.*

**Ground-truth strategy for the bi-temporal split (zero manual annotation).** Take a Wikipedia
infobox attribute. The revision history tells you exactly which value was live at any date and
exactly when it changed. Generate the valid-time question — *"What was X's ⟨attribute⟩ on ⟨date⟩?"*
— with the gold answer read from the revision live at that date. Then generate its transaction-time
twin from the same log — *"As of ⟨date⟩, what did the record say X's ⟨attribute⟩ was?"* Where a fact
was corrected retroactively, the two answers differ. **That difference set is the bi-temporal
separation benchmark nobody has published**, and it maps directly onto the "did we know about the
change at the time" half of your example query.

---

## 2. Sanctions Compliance over a Temporal Graph

**End user:** Sanctions compliance and financial-crime analysts in banks, payment processors, trade
finance, shipping and export-control teams.

**Real-world use case:** A bank processed a $2 million payment to Global Trade Corp on 14 March
2023. Today, in 2026, the bank is doing a retrospective compliance check and notices that Global
Trade Corp appears on today's EU sanctions list. The bank must now prove whether that entity was
actually sanctioned on the payment date. The bank has to consult four different sanctions
authorities — OFAC (US), EU, UK and UN — each with their own lists updated on different schedules,
and their own rules for **effective date** (when the sanction legally starts) versus **publication
date** (when it was announced).

**Example query:**
> Was Global Trade Corp designated by OFAC, EU, UK or UN on 14 March 2023? Show the effective date,
> publication date, and any amendments or delistings that affect that date for each authority.

**Business decision:** Determine whether the $2M payment violated any sanctions regime, then freeze
the beneficiary account if the payment was illegal, reverse the payment if it is still in the
banking system, and file a Suspicious Activity Report with the financial intelligence unit.

> ### ⚠️ The finding that makes this use case worth doing
>
> **No sanctions authority publishes point-in-time snapshots of its own list.** OFAC states
> explicitly that it *"does not maintain 'historic' or 'archived' versions of its actual lists"* —
> only active versions, for policy and legal reasons. The UN Consolidated List page states that the
> current version *"supersedes all previous versions."* The EU FSF and UK OFSI publish current
> lists only.
>
> **The query above therefore cannot be answered from the primary sources by download.** It can
> only be answered by *reconstructing* the list as of a past date — replaying the change archives
> forward. OFAC maintains those change records **back to 1994** (PDF from 2001, TXT from 1994) and
> publishes browsable **delta files by year** through its Sanctions List Service.
>
> That is not a data-collection inconvenience. It *is* the research problem, it is exactly what a
> bi-temporal graph is for, and it is why "just query the current list" is not a competing solution.

### Relevant research papers

| Research | What it demonstrates | Scale | What is missing |
|---|---|---|---|
| **OpenSanctions Pairs** — Smith, Sesodia, Lindenberg & Schroeder de Witt, arXiv:2603.11051 (Feb 2026) | The only large-scale benchmark built on **real sanctions data and real analyst deduplication decisions**. Production rule-based matcher (nomenklatura RegressionV1) **91.33% F1**; GPT-4o **98.95%**; DeepSeek-R1-Distill-Qwen-14B **98.23%** | **755,540 labeled pairs, 293 sources, 31 countries** | Pairwise entity matching only — **no graph traversal, no time dimension, no question answering**. The authors note pairwise matching is near its ceiling and the bottleneck has moved to blocking and clustering. |
| **RAGulating Compliance: A Multi-Agent Knowledge Graph for Regulatory QA** — MasterControl AI Research, arXiv:2508.09893 (Aug 2025) | **Closest work on your architecture.** Agents extract SPO triplets from regulatory documents, then clean, normalise, deduplicate and update an ontology-free KG; triplets embedded with source sections and metadata in one store; orchestrated agent pipeline does triplet-level retrieval with traceability and subgraph visualisation | Regulatory corpora; scale not reported | **No temporal versioning** — "updating" the KG overwrites rather than invalidates, so a superseded designation simply disappears. No effective-date / publication-date distinction. Not sanctions. No distributed execution. |
| **Temporal Misgrounding in Legal RAG** — Cymbler, Guez & Fabre, **ICML 2026 Workshop on AI for Law**, arXiv:2608.09393 | Names and quantifies **temporal misgrounding**: systematically retrieving the currently in-force version when the applicable version is earlier or later — the exact failure mode of checking today's list for a 2023 payment. Argues legal QA is a temporally-indexed retrieval problem | 32,436 article-versions, 93 years, 209 expert questions (FiscalQA Pro) | Retrieval-only, **no graph**. **One authority, one jurisdiction** — no conflicting timelines across independent issuing bodies. Legal text, not entity designations. No extraction: the versioned corpus is given, not built. |

*Also considered, cut for brevity: GraphCompliance (arXiv:2510.26309, policy+context graph
alignment, 300 GDPR scenarios); Kim & Yang, Frontiers in AI, Nov 2024 (the only peer-reviewed
sanctions-screening study — NLP raised sensitivity 48.18%→70.96% but dropped accuracy
66.60%→47.80%, useful as motivation for the false-positive burden); An Ontology-Driven Graph RAG
for Legal Norms (JURIX 2025); LDBC FinBench (PVLDB 18, 2025) as the graph-systems benchmark;
AMLworld and Elliptic for the transaction layer.*

### The gap, stated plainly

**There is no public temporal question-answering benchmark over sanctions data, and no published
research on point-in-time sanctions reconstruction.** The sanctions benchmark that exists has no
time dimension. The multi-agent regulatory KG that exists overwrites instead of invalidating. The
temporal-versioning work that exists covers a single authority's legal text. **Nothing reconciles
four independent authorities with differing effective-date and publication-date conventions into
one queryable timeline.** That is the contribution.

### Datasets

| # | Source | What it gives you | Size / scale | Access & licence |
|---|---|---|---|---|
| 1 | **OpenSanctions** — [bulk](https://www.opensanctions.org/docs/bulk/) · [historical & deltas](https://www.opensanctions.org/docs/bulk/updates/) · [statements](https://www.opensanctions.org/docs/statements/) | **Core graph and gold labels.** Aggregates all four authorities into one model. **Dated snapshots**: `data.opensanctions.org/datasets/YYYYMMDD/default/entities.ftm.json`, history from ~**July 2021**. **Delta files** carry additions, modifications and removals with explicit `DEL` operations. The statement model records per-assertion provenance, mapping onto `(s, r, o, t_s, t_e, t_p, d, c)` | ~1.7M entities, 293 sources, 31 countries | **CC BY-NC 4.0** — free for non-commercial use with attribution. ⚠️ Data older than a few months **requires a data delivery token**; request academic access now |
| 2 | **OFAC** — [Archive of Changes to the SDN List](https://home.treasury.gov/policy-issues/financial-sanctions/specially-designated-nationals-list-sdn-list/archive-of-changes-to-the-sdn-list) · [Sanctions List Service](https://ofac.treasury.gov/sanctions-list-service) | **The reconstruction source, and independent validation of your method.** Change archives in **PDF from 2001 and TXT from 1994**; the Sanctions List Service publishes **browsable delta files by year**. Current list in XML, fixed-field and delimited formats | Change records back to **1994** — 27 years deeper than OpenSanctions | US Government, public domain. ⚠️ No historic list snapshots exist — the list at a past date must be reconstructed by replaying deltas |
| 3 | **The other three authority feeds** — [EU FSF](https://www.opensanctions.org/datasets/eu_fsf/) (source XML at `webgate.ec.europa.eu/fsd/fsf`) · [UK OFSI consolidated list](https://www.gov.uk/government/publications/financial-sanctions-consolidated-list-of-targets) · [UN SC Consolidated List](https://main.un.org/securitycouncil/en/content/un-sc-consolidated-list) | **The multi-authority dimension the query demands.** Three independent issuing bodies, three update cadences, three conventions for effective versus publication date. Grouped as one entry because they are the same kind of artefact: current-state official feeds | EU FSF **15,938 entities** (4,401 people, 1,725 organisations, 2,117 addresses, 2 vessels), daily. UN **684 individuals, 193 entities**, daily | All free. EU: XML/CSV. UK: XML, CSV, Excel, HTML, PDF, plain text. UN: XML, HTML, PDF. ⚠️ All three publish **current state only** |

*Also considered, cut for brevity: OpenSanctions Pairs (755,540 labeled pairs) for evaluating the
entity-resolution stage against published baselines; the ownership layer (UK Companies House PSC,
Open Ownership BODS, GLEIF Level 2, ICIJ Offshore Leaks) if indirect control re-enters scope;
AMLworld and Elliptic for the payment-legality half.*

**Ground-truth strategy — two independent sources, which is unusually strong:**

1. **OpenSanctions deltas (2021→now).** Every **ADD** yields gold answers for *"when was X
   designated?"* and *"was X sanctioned on date d?"* across the whole timeline. Every **DEL** yields
   delisting ground truth — and these are the questions that break single-timeline systems, since a
   naive current-state graph answers "no" for every date. Every **MOD** yields *"what changed about
   X, and when?"* The listing-date versus publication-date fields inside the FollowTheMoney records
   give the valid-time / transaction-time split with no annotation.
2. **OFAC change archives (1994→now).** An independent, authority-native reconstruction going back
   **27 years further**. Cross-checking a reconstructed OFAC list against the OpenSanctions snapshot
   for the same date **validates your reconstruction method** — and any disagreement is itself a
   finding worth reporting.

**Accuracy limitation — state this honestly.** An OpenSanctions delta records when *OpenSanctions
observed* a change: transaction time relative to the aggregator, not the authority's effective date
and not its publication date. OFAC's archives are closer to the source but are change notices, not
list states, so a reconstruction accumulates error over 30 years of replay. Designations are also
sometimes given retroactive effect. This is a real limitation of the ground truth — and, exactly as
`redteam.txt` labels only some attack stages in the cyber use case, it is also the reason
bi-temporal modelling is required rather than optional.

### What you can measure

Directly parallel to the AIT alert-dataset measurements in the cyber use case:

- Whether the correct **designation state** is returned for a given as-of date, per authority.
- Whether the system correctly separates **effective date from publication date**, and reports both
  when they differ.
- Whether every retrieved fact's **validity interval contains the query date** (temporal
  precision@k).
- Whether the system distinguishes **"not sanctioned"** from **"not yet recorded"** — the
  valid-time versus transaction-time separation.
- Whether **designation, amendment and delisting events are returned in the correct order** across
  the four authorities.
- Whether **multiple authorities improve** reconstruction over any single authority.
- **What happens when one authority's feed is unavailable** — the resilience test, matching the
  "one alert source unavailable" experiment in the cyber use case.
- **Reconstruction fidelity:** replayed OFAC list at date *d* versus the OpenSanctions snapshot at
  date *d*.
- **False-positive rate at fixed recall**, against the nomenklatura baseline (91.33% F1).

---

## 3. Honest risks

1. **Extraction cost at 1 GB dominates use case 1.** Published estimates put GraphRAG-style
   indexing at tens of thousands of dollars for multi-gigabyte corpora. Decide the hybrid strategy —
   atomic-fact decomposition (ATOM), dependency parsing (arXiv:2507.03226) or entity-only linking
   (LinearRAG) for the bulk, LLM only for hard cases — and measure cost per 100 MB before
   committing to the 1 GB figure.
2. **OpenSanctions historical access needs a data delivery token.** Request it now.
3. **OFAC reconstruction is real engineering work, not a download.** Budget for it, and treat
   reconstruction fidelity as a reported result rather than an assumption.
4. **Benchmark contamination.** Wikipedia- and news-derived temporal QA sets are in every model's
   pretraining. Always report a closed-book baseline so you can show retrieval is doing the work.
5. **JasmineGraph storage constraints** identified separately (no in-place property update in the
   native store; `src/temporalstore/` is a snapshot-window model with no wall-clock mapping or
   validity intervals; no deletion in the live path) determine whether the temporal model is
   append-only or mutating. Settle that before the schema.
