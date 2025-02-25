import json
import http.server
import socketserver

def modify_html():
    search_str = 'o=[i("manifest","manifest.json"),i("catalog","catalog.json")]'

    with open('target/index.html', 'r') as f:
        content_index = f.read()
        
    with open('target/manifest.json', 'r') as f:
        json_manifest = json.loads(f.read())

    with open('target/catalog.json', 'r') as f:
        json_catalog = json.loads(f.read())
        
    with open('target/dbt_docs_parking_violations.html', 'w') as f:
        new_str = "o=[{label: 'manifest', data: " + json.dumps(json_manifest) + "},{label: 'catalog', data: " + json.dumps(json_catalog) + "}]"
        new_content = content_index.replace(search_str, new_str)
        f.write(new_content)

def run_server():
    PORT = 8000
    Handler = http.server.SimpleHTTPRequestHandler
    httpd = socketserver.TCPServer(("", PORT), Handler)
    print(f"Serving at port {PORT}")
    httpd.serve_forever()

if __name__ == "__main__":
    modify_html()
    run_server()




