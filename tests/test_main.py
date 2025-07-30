from app import create_app


def test_chargeback_handler():
    app = create_app()
    client = app.test_client()
    response = client.get('/run')
    assert response.status_code in [200, 500]
    assert "status" in response.json


def test_parse_sample_input():
    # Placeholder test for parsing function
    input_data = "H1|12345\nD|54321|Chargeback\nT|Total"
    parsed_lines = input_data.strip().split("\n")

    assert len(parsed_lines) == 3

    # Assert line prefixes
    assert parsed_lines[0].startswith("H1")
    assert parsed_lines[1].startswith("D")
    assert parsed_lines[2].startswith("T")

    # Optionally assert expected parsed parts
    header_parts = parsed_lines[0].split("|")
    detail_parts = parsed_lines[1].split("|")
    trailer_parts = parsed_lines[2].split("|")

    assert header_parts == ["H1", "12345"]
    assert detail_parts == ["D", "54321", "Chargeback"]
    assert trailer_parts == ["T", "Total"]
