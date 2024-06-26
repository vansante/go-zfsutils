package zfs

type PropertySource string

type PropertySources []PropertySource

func (ps PropertySources) StringSlice() []string {
	strs := make([]string, len(ps))
	for i, p := range ps {
		strs[i] = string(p)
	}
	return strs
}

const (
	PropertySourceLocal     PropertySource = "local"
	PropertySourceInherited PropertySource = "inherited"
	PropertySourceTemporary PropertySource = "temporary"
	PropertySourceReceived  PropertySource = "received"
	PropertySourceDefault   PropertySource = "default"
)

const (
	PropertyAvailable          = "available"
	PropertyCanMount           = "canmount"
	PropertyCompression        = "compression"
	PropertyEncryption         = "encryption"
	PropertyEncryptionRoot     = "encryptionroot"
	PropertyFilesystemCount    = "filesystem_count"
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
)

const (
	ValueYes   = "yes"
	ValueOn    = "on"
	ValueNo    = "no"
	ValueOff   = "off"
	ValueNone  = "none"
	ValueUnset = "-"
)

const (
	EncryptionAES128CCM = "aes-128-ccm"
	EncryptionAES192CCM = "aes-192-ccm"
	EncryptionAES256CCM = "aes-256-ccm"
	EncryptionAES128GCM = "aes-128-gcm"
	EncryptionAES192GCM = "aes-192-gcm"
	EncryptionAES256GCM = "aes-256-gcm"
)

const (
	KeyFormatHex        = "hex"
	KeyFormatPassphrase = "passphrase"
	KeyFormatRaw        = "raw"
)

const (
	KeyLocationPrompt  = "prompt"
	KeyStatusAvailable = "available"
)

const CanMountNoAuto = "noauto"
