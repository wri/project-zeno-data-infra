from mcp_server.chat.mcp_chat import show_tool_results


def tool_response(content, is_error=False):
    return {
        "role": "user",
        "content": [
            {
                "type": "tool_result",
                "tool_use_id": "call-1",
                "content": content,
                "is_error": is_error,
            }
        ],
    }


def test_text_blocks_are_printed(capsys):
    show_tool_results(tool_response([{"type": "text", "text": '{"area_ha": 12.5}'}]))

    assert '{"area_ha": 12.5}' in capsys.readouterr().out


def test_long_results_are_cut_short(capsys):
    show_tool_results(tool_response("x" * 5000), max_chars=100)

    output = capsys.readouterr().out
    assert "x" * 100 in output
    assert "x" * 101 not in output
    assert "4900 more characters" in output


def test_errors_are_marked(capsys):
    show_tool_results(tool_response("Analytics API returned 404", is_error=True))

    assert "[tool error]" in capsys.readouterr().out
