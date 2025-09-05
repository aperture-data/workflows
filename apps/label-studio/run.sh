vars=()
vars+=(--rm -it)
vars+=(--add-host host.docker.internal:host-gateway )
vars+=(-e DB_HOST="host.docker.internal")
vars+=(-e WF_LOG_LEVEL="info")
vars+=(-e WF_LABEL_STUDIO_USER=aperturedb@localhost )
vars+=(-e WF_LABEL_STUDIO_PASSWORD=41apertureDB3 )
vars+=(-e WF_LABEL_STUDIO_TOKEN=PXq08K1kCwg9eTmhFPdwOgE5DEVvy5MejfW26p13EQvkse6w)
vars+=(-e WF_LABEL_STUDIO_URL_PATH="http://localhost:9000/labelstudio")
#vars+=(-e DEBUG="true")

vars+=(-p 9000:8000 ) # export LS running on 8000.
vars+=(-p 8080:8080 ) # export status on 8080
vars+=(aperturedata/workflows-label-studio)
docker run "${vars[@]}"
