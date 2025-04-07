import os
import shutil
import jieba
import numpy as np
from gensim.models import Word2Vec
from langchain_community.document_loaders import TextLoader, PyPDFium2Loader, CSVLoader, Docx2txtLoader
from langchain_community.vectorstores import Chroma
from langchain_core.documents import Document
from langchain_core.prompts import ChatPromptTemplate
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_core.embeddings import Embeddings

from my_qianfan_llm import llm

class CustomEmbeddings(Embeddings):
    def __init__(self, model_path='word2vec.model'):
        self.model_path = model_path
        self.model = None
        self.vector_size = 100  # 向量维度
        
    def _train_model(self, texts):
        # 分词
        sentences = [list(jieba.cut(text)) for text in texts]
        # 训练模型
        self.model = Word2Vec(sentences, vector_size=self.vector_size, window=5, min_count=1)
        # 保存模型
        self.model.save(self.model_path)
        
    def _load_model(self):
        if os.path.exists(self.model_path):
            self.model = Word2Vec.load(self.model_path)
            return True
        return False
        
    def _get_text_vector(self, text):
        words = list(jieba.cut(text))
        vectors = []
        for word in words:
            if word in self.model.wv:
                vectors.append(self.model.wv[word])
        if vectors:
            return np.mean(vectors, axis=0)
        return np.zeros(self.vector_size)
        
    def embed_documents(self, texts):
        if not self.model:
            if not self._load_model():
                self._train_model(texts)
        return [self._get_text_vector(text).tolist() for text in texts]
        
    def embed_query(self, text):
        if not self.model:
            if not self._load_model():
                self._train_model([text])
        return self._get_text_vector(text).tolist()

# 使用自定义的文本向量化方法
embeddings = CustomEmbeddings()


def create_chroma_db(db_name, temp_source_file_path):
    persist_directory = "./dbs/" + db_name

    # Check if the Chroma database already exists
    if os.path.exists(persist_directory):
        print(f"Chroma database '{db_name}' already exists.")
        return "Chroma database already exists"

    base_dir = "./can_get_dbs/"
    full_path = os.path.join(base_dir, db_name)

    # Ensure the base directory exists
    if not os.path.exists(base_dir):
        os.makedirs(base_dir)

    # Create the subdirectory
    if not os.path.exists(full_path):
        os.mkdir(full_path)

    # os.mkdir("./can_get_dbs/" + db_name)

    # 获取文件扩展名以决定使用哪一个Loader
    extension = temp_source_file_path.split(".")[-1]
    loader = None
    if extension == 'txt':
        loader = TextLoader(temp_source_file_path, encoding='utf8')
    elif extension == 'pdf':
        loader = PyPDFium2Loader(temp_source_file_path)
    elif extension == 'docx':
        loader = Docx2txtLoader(temp_source_file_path)
    elif extension == 'csv':
        loader = CSVLoader(temp_source_file_path)
    else:
        print(f"Unsupported file extension: '{extension}'")
        return f"Unsupported file extension: '{extension}'"

    docs = loader.load()
    text_splitter = RecursiveCharacterTextSplitter(chunk_size=1000, chunk_overlap=200)
    splits = text_splitter.split_documents(docs)
    Chroma.from_documents(documents=splits, embedding=embeddings, persist_directory=persist_directory)
    # 删除临时文件
    try:
        os.remove(temp_source_file_path)
    except Exception as e:
        print(f"Error deleting temporary file: {e}")

    return "ok"


def format_docs(docs):
    return "\n\n".join(doc.page_content for doc in docs)


def answer_user_query(db_name, question):
    persist_directory = "./dbs/" + db_name
    vectorstore = Chroma(embedding_function=embeddings, persist_directory=persist_directory)
    retriever = vectorstore.as_retriever()

    prompt_template = ChatPromptTemplate.from_messages(
        [
            ("system",
             "先判断一下用户的问题和参考内容是否有关，如果有关，则结合参考内容进行回答，如果无关，则请你像买看见参考内容一样即可。即若是user的问题和参考内容无关，则不要回答参考内容相关的信息，问题和参考内容是否有关系对用户来说是不可感知的\n##强调：你不要在回答中画蛇添足"),
            ("user", "我的问题是:{question}，\n可参考以下内容：{context}")
        ]
    )

    return llm.stream(prompt_template.invoke(
        {"context": format_docs(retriever.invoke(question)), "question": question}
    ).to_messages())


def answer_without_chroma_db(question):
    prompt_template = ChatPromptTemplate.from_messages(
        [
            ("user", "{question}")
        ]
    )

    return llm.stream(prompt_template.invoke(
        {"question": question}
    ).to_messages())


def delete_chroma_db(db_name):
    persist_directory = "./can_get_dbs/" + db_name

    try:
        if os.path.exists(persist_directory):
            shutil.rmtree(persist_directory, ignore_errors=True)
            print(f"成功强制删除目录:  {persist_directory}")
        else:
            print(f"目录  {persist_directory}  不存在")
    except  Exception as e:
        print(f"强制删除目录时出错:  {e}")

    return "ok"


def get_all_segments(db_name):
    persist_directory = "./dbs/" + db_name
    vectorstore = Chroma(embedding_function=embeddings, persist_directory=persist_directory)
    res = vectorstore.get()
    return res


def delete_segments_by_id(db_name, id):
    persist_directory = "./dbs/" + db_name
    vectorstore = Chroma(embedding_function=embeddings, persist_directory=persist_directory)
    print(vectorstore.delete(ids=[id]))
    return "ok"


def update_segments_by_id(db_name, id, new_content, metedata_source):
    persist_directory = "./dbs/" + db_name
    vectorstore = Chroma(embedding_function=embeddings, persist_directory=persist_directory)
    print(vectorstore.update_document(document_id=id, document=Document(page_content=new_content,
                                                                        metadata={"source": metedata_source})))
    return "ok"


def add_new_segments(db_name, new_content, metedata_source):
    persist_directory = "./dbs/" + db_name
    vectorstore = Chroma(embedding_function=embeddings, persist_directory=persist_directory)
    return vectorstore.add_documents(
        documents=[Document(page_content=new_content, metadata={"source": metedata_source})])
