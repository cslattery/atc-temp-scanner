# Temp collector

Bleak: https://bleak.readthedocs.io/en/latest/scanning.html





# Local development

## Create pubsub emulator
from: https://cloud.google.com/pubsub/docs/emulator
```bash
# specify host-port becasue ipv6 appears to not work on my ubuntu pc
gcloud beta emulators pubsub start --project='emulatorproject' --host-port=127.0.0.1:8085
$(gcloud beta emulators pubsub env-init)
```
 