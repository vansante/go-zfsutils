package zfs

const (
	PropertyAvailable          = "available"
	PropertyCanMount           = "canmount"
	PropertyCompression        = "compression"
	PropertyEncryption         = "encryption"
	PropertyKeyFormat          = "keyformat"
	PropertyKeyStatus          = "keystatus"
	PropertyKeyLocation        = "keylocation"
	PropertyLogicalUsed        = "logicalused"
	PropertyMounted            = "mounted"
	PropertyMountPoint         = "mountpoint"
	PropertyName               = "name"
	PropertyOrigin             = "origin"
	PropertyQuota              = "quota"
	PropertyReferenced         = "referenced"
	PropertyRefQuota           = "refquota"
	PropertyReadOnly           = "readonly"
	PropertyReceiveResumeToken = "receive_resume_token"
	PropertyType               = "type"
	PropertyUsed               = "used"
	PropertyUsedByDataset      = "usedbydataset"
	PropertyVolSize            = "volsize"
	PropertyWritten            = "written"

	PropertyYes   = "yes"
	PropertyOn    = "on"
	PropertyNo    = "no"
	PropertyOff   = "off"
	PropertyUnset = "-"

	EncryptionAES128CCM = "aes-128-ccm"
	EncryptionAES192CCM = "aes-192-ccm"
	EncryptionAES256CCM = "aes-256-ccm"
	EncryptionAES128GCM = "aes-128-gcm"
	EncryptionAES192GCM = "aes-192-gcm"
	EncryptionAES256GCM = "aes-256-gcm"

	KeyFormatHex        = "hex"
	KeyFormatPassphrase = "passphrase"
	KeyFormatRaw        = "raw"

	KeyLocationPrompt  = "prompt"
	KeyStatusAvailable = "available"

	CanMountNoAuto = "noauto"
)
