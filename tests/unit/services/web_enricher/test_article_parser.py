from __future__ import annotations

from services.web_enricher.article_parser import ArticleParser


def test_html_parser_extracts_metadata_excerpt_and_outbound_links() -> None:
    parser = ArticleParser(excerpt_chars=80, max_outbound_links=10)

    parsed = parser.parse(
        final_url="https://example.com/posts/ai",
        content_type="text/html",
        body_text="""
        <html>
          <head>
            <title>Example AI Post</title>
            <meta name="description" content="A short description">
            <meta property="og:site_name" content="Example">
            <meta name="author" content="Dev Author">
            <meta property="article:published_time" content="2026-04-28T00:00:00Z">
            <link rel="canonical" href="/canonical/ai">
          </head>
          <body>
            <article>
              <h1>Example AI Post</h1>
              <p>This article describes a GitHub project and an X post.</p>
              <a href="https://github.com/openai/openai-python">Repo</a>
              <a href="https://x.com/dev/status/1881234567890123456">Post</a>
            </article>
          </body>
        </html>
        """,
    )

    assert parsed.title == "Example AI Post"
    assert parsed.description == "A short description"
    assert parsed.site_name == "Example"
    assert parsed.author == "Dev Author"
    assert parsed.published_at is not None
    assert parsed.canonical_url_candidate == "https://example.com/canonical/ai"
    assert "GitHub project" in (parsed.main_text_excerpt or "")
    assert parsed.outbound_links == [
        "https://github.com/openai/openai-python",
        "https://x.com/dev/status/1881234567890123456",
    ]


def test_plain_text_parser_uses_first_non_empty_line_and_url_regex() -> None:
    parser = ArticleParser(excerpt_chars=120, max_outbound_links=5)

    parsed = parser.parse(
        final_url="https://example.com/plain",
        content_type="text/plain",
        body_text="\nPlain title\nSee https://github.com/openai/openai-python for details.",
    )

    assert parsed.title == "Plain title"
    assert parsed.main_text_excerpt == "Plain title See https://github.com/openai/openai-python for details."
    assert parsed.outbound_links == ["https://github.com/openai/openai-python"]
    assert parsed.normalized_projection == {"plain_text_mode": True}
