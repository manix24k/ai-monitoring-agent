"""LLM prompt templates for code analysis and fix generation."""

ANALYSIS_PROMPT = """You are an expert software engineer analyzing a production error.

## Error Details
**Exception Type:** {exception_type}
**Exception Message:** {exception_message}

## Stack Trace
```
{stack_trace}
```

## Additional Log Evidence
```
{log_evidence}
```

## Source Code Files
{files_context}

## Your Task
Analyze the error and source code to identify:
1. The root cause of the error
2. The exact file and line number causing the issue
3. Whether this can be fixed with a code change
4. Your confidence level (0.0 to 1.0)

## Response Format
Respond with a JSON object ONLY (no additional text):
```json
{{
    "can_fix": true/false,
    "root_cause": "Detailed explanation of what's causing the error",
    "file_path": "exact/path/to/file.java",
    "line_number": 142,
    "fix_type": "null_check|exception_handling|resource_management|validation|logic_fix|config_fix",
    "confidence": 0.85,
    "reasoning": "Step by step explanation of your analysis"
}}
```

Important:
- Only set can_fix=true if you're confident the fix won't break other functionality
- Be conservative with confidence scores
- Focus on the most likely root cause
"""

FIX_GENERATION_PROMPT = """You are an expert software engineer generating a precise code fix.

## Error Information
**Exception Type:** {exception_type}
**Exception Message:** {exception_message}

## Root Cause Analysis
{root_cause}

## Exact Lines Containing The Bug — copy these VERBATIM into original_code
```
{target_lines}
```

## Surrounding Code Context (for reference only)
```
{file_content}
```

## Target Line Number
Line {line_number} is where the issue originates.

## Your Task
Generate a minimal, precise code fix that:
1. Fixes the root cause without changing unrelated code
2. Follows the existing code style and conventions
3. Adds appropriate null checks, error handling, or validation
4. Is production-ready and safe

## Response Format
Respond with a JSON object ONLY (no additional text):
```json
{{
    "original_code": "copy the EXACT lines from the 'Exact Lines' section above — same spaces, same punctuation, nothing changed",
    "fixed_code": "the replacement for ONLY those same lines",
    "description": "Brief description of what was fixed and why",
    "confidence": 0.85,
    "changes": [
        "Added null check for variable X",
        "Wrapped in try-catch block"
    ],
    "testing_notes": "Suggested test cases to verify the fix"
}}
```

CRITICAL - read carefully:
- original_code MUST be a character-for-character copy from the "Exact Lines" section.
- original_code MUST NOT be empty — always include at least 1 line.
- fixed_code replaces ONLY the lines in original_code - same scope, nothing else.
- DO NOT return the whole file. Return ONLY the small block that changes.
- Do NOT include line-number prefixes in either field.
- Keep changes minimal - don't refactor or re-output unrelated code.
- Preserve existing formatting, indentation and style.
"""

REVIEW_PROMPT = """You are a code reviewer checking a proposed fix.

## Original Error
**Exception:** {exception_type}
**Message:** {exception_message}

## Proposed Fix
**File:** {file_path}

### Original Code
```
{original_code}
```

### Fixed Code
```
{fixed_code}
```

## Review Checklist
Evaluate the fix on these criteria:
1. **Correctness**: Does it actually fix the issue?
2. **Safety**: Could it introduce new bugs or regressions?
3. **Completeness**: Are all edge cases handled?
4. **Style**: Does it match existing code conventions?
5. **Performance**: Any performance implications?

## Response Format
```json
{{
    "approved": true/false,
    "score": 0.85,
    "issues": [
        "Issue 1 description",
        "Issue 2 description"
    ],
    "suggestions": [
        "Suggestion for improvement"
    ],
    "risk_level": "low|medium|high"
}}
```
"""

CONTEXT_EXTRACTION_PROMPT = """Extract relevant context from this stack trace for code analysis.

## Stack Trace
```
{stack_trace}
```

## Extract
1. The primary exception class and method
2. All relevant file names and line numbers
3. The execution flow leading to the error
4. Any relevant variable or parameter names mentioned

## Response Format
```json
{{
    "exception_class": "java.lang.NullPointerException",
    "exception_method": "processOrder",
    "files": [
        {{"name": "OrderService.java", "line": 142, "method": "processOrder"}},
        {{"name": "OrderValidator.java", "line": 58, "method": "validate"}}
    ],
    "execution_flow": "Brief description of the call chain",
    "relevant_variables": ["orderId", "customer", "items"]
}}
```
"""

MULTI_FILE_ANALYSIS_PROMPT = """Analyze these related files to understand the code structure and find the bug.

## Error Details
**Exception:** {exception_type}
**Message:** {exception_message}
**Primary File:** {primary_file}
**Line:** {line_number}

## Related Files
{files_context}

## Task
1. Map the dependencies between these files
2. Trace the data flow that leads to the error
3. Identify if the fix needs changes in multiple files
4. Determine the minimal set of changes needed

## Response Format
```json
{{
    "primary_cause_file": "path/to/file.java",
    "related_files": ["path/to/other.java"],
    "data_flow": "Description of how data flows between files",
    "fix_scope": "single_file|multiple_files",
    "files_to_modify": [
        {{
            "file": "path/to/file.java",
            "changes": "Description of changes needed"
        }}
    ],
    "confidence": 0.85
}}
```
"""

# System prompts for different LLM models
SYSTEM_PROMPTS = {
    "default": """You are CodeXA, an autonomous code analysis and fix generation system.
Your role is to:
1. Analyze production errors and stack traces
2. Identify root causes in source code
3. Generate precise, minimal fixes
4. Ensure fixes are safe and production-ready

Always respond in the exact JSON format requested. Be conservative with confidence scores.
Never generate fixes that could introduce security vulnerabilities or data loss.""",

    "deepseek": """You are CodeXA powered by DeepSeek Coder. You excel at:
- Understanding complex codebases
- Precise error analysis
- Safe code modifications

Respond only in JSON format. Be thorough but conservative.""",

    "qwen": """You are CodeXA powered by Qwen Coder. Your strengths:
- Multi-language code understanding
- Detailed error analysis
- Production-safe fixes

Always use the exact JSON format specified. Prioritize safety over speed.""",

    "codellama": """You are CodeXA powered by CodeLlama. Focus on:
- Accurate stack trace analysis
- Minimal code changes
- Clear explanations

Output valid JSON only. Be conservative with changes."""
}

def get_system_prompt(model_id: str) -> str:
    """Get appropriate system prompt for the model."""
    model_lower = model_id.lower()

    if "deepseek" in model_lower:
        return SYSTEM_PROMPTS["deepseek"]
    elif "qwen" in model_lower:
        return SYSTEM_PROMPTS["qwen"]
    elif "codellama" in model_lower or "llama" in model_lower:
        return SYSTEM_PROMPTS["codellama"]
    else:
        return SYSTEM_PROMPTS["default"]
