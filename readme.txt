[Backuper の使い方]

java -jar jarfiile.jar [-dsl] DicFolder [SrcName] [DstName]

DicFolder : 辞書ファイルやログファイルの格納場所
SrcName   : コピー元「識別名」を指定する。
DstName   : コピー先「識別名」を指定する。
-d        : デバッグモード。スキャンやコピーは行わず、DBの内容だけでcopy予定を表示する。
-s	  : スキップスキャン。スキャンを省略する。
-l	  : 定義ファイルの内容を出力する。

・DicFolder 以下の "folders.conf" ファイルに設定情報を記載する。
・DstName が省略されたら、SrcName に対応する辞書の更新のみを行う。

・シンボリックリンクは手繰らない。

[folders.conf の記載内容]

<識別名1>=<フォルダ名1>
<無視するファイルパターン1>
<無視するファイルパターン2>
...
<識別名2>=<フォルダ名2>
...

<識別名> : 実行時の引数に指定する
<フォルダ名> : フォルダをフルパスで指定する
<無視するファイルパターン> : コピーしないファイルパターン
・'/' で始まるものは、パスを見てマッチする。
・'/' で始まらないものは、パスは無視する。
・'/' で終わるものは、フォルダにマッチする。
・'/' で終わらないものは、ファイルにマッチする。
・'**' は０個以上ののフォルダ・ファイルにマッチする。
・'*' は１つのフォルダ・ファイルにマッチする。

[サンプル定義ファイル]

linux.home.src=/home/junsei	# 識別名 "linux.home.src" のフォルダ名
/work/*/target/			# 無視するフォルダパス
#*#				# 無視するファイル名
*~
/.cache/
/.m2/
/.config/
/.emacs.d/semanticdb/
/.bash_history
/.local/share/gvfs-metadata/

linux.dst1=/mnt/C/BACKUP/Linux/home/junsei

linux.dst2=/mnt/D/Linux/home/junsei

[エラー対応方法]
"CANNOT COPY" と表示されるのは、コピー先に無視する対象のファイルがあり、上書きコピーできないためである。
内容を確認し、不必要なら手で削除をして、再度バックアップ処理をすること。
