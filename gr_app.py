import sqlite3

import gradio as gr


def greet(name):
    return "Hello " + name + "!"

def query_id(user_id):
    # 连接数据库
    conn = sqlite3.connect("user.db")
    print("Opened database successfully")
    c = conn.cursor()
    # 查询数据
    cursor = c.execute("SELECT * from user_profile where id = " + user_id)
    for row in cursor:
        print("ID = ", row[0])
        print("趋炎附势 = ", row[1])
        print("喜新厌旧 = ", row[2])
    return str(row)


with gr.Blocks() as demo:
    user_id = gr.Textbox(label="user_id")
    user_id.change(fn=query_id, inputs=user_id, outputs=user_id, api_name="query_id")

    user_operation_diary = gr.Textbox(label="Diary")
    user_operation_diary_btn = gr.Button("Diary")
    user_operation_diary_btn.click()

    item_list = gr.Textbox(label="Item List")
    item_list_btn = gr.Button("Item List")

    output = gr.Textbox(label="Output Box")
    greet_btn = gr.Button("Greet")
    greet_btn.click(fn=greet, inputs=name, outputs=output, api_name="greet")

demo.launch()
