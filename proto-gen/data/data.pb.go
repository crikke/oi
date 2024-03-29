// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.28.1
// 	protoc        v3.21.4
// source: proto/data/data.proto

package data

import (
	reflect "reflect"
	sync "sync"

	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

// Record contains a mutation which has been persisted to disk.
//
// To ensure that records can be replayed in the correct order, each record will receive an monotonic
// log sequence number (LSN). The LSN is a 64bit unsigned integer which the first 32bits specify in which
// log segment file the record exist and the last 32 bits specify the records index in the file.
//
// When persisting the record the checksum of the mutation is calculated and persisted aswell (CRC), in order
// to ensure that the data is valid when replaying the records. The persisted checksum is compared to the
// checksum of the data field
type Record struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	LSN      uint64    `protobuf:"varint,1,opt,name=LSN,proto3" json:"LSN,omitempty"`
	Data     *Mutation `protobuf:"bytes,2,opt,name=Data,proto3" json:"Data,omitempty"`
	Checksum uint32    `protobuf:"varint,3,opt,name=Checksum,proto3" json:"Checksum,omitempty"`
}

func (x *Record) Reset() {
	*x = Record{}
	if protoimpl.UnsafeEnabled {
		mi := &file_proto_data_data_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Record) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Record) ProtoMessage() {}

func (x *Record) ProtoReflect() protoreflect.Message {
	mi := &file_proto_data_data_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Record.ProtoReflect.Descriptor instead.
func (*Record) Descriptor() ([]byte, []int) {
	return file_proto_data_data_proto_rawDescGZIP(), []int{0}
}

func (x *Record) GetLSN() uint64 {
	if x != nil {
		return x.LSN
	}
	return 0
}

func (x *Record) GetData() *Mutation {
	if x != nil {
		return x.Data
	}
	return nil
}

func (x *Record) GetChecksum() uint32 {
	if x != nil {
		return x.Checksum
	}
	return 0
}

type Mutation struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Key       []byte     `protobuf:"bytes,1,opt,name=Key,proto3" json:"Key,omitempty"`
	Value     []byte     `protobuf:"bytes,2,opt,name=Value,proto3" json:"Value,omitempty"`
	Tombstone *Tombstone `protobuf:"bytes,3,opt,name=Tombstone,proto3" json:"Tombstone,omitempty"`
}

func (x *Mutation) Reset() {
	*x = Mutation{}
	if protoimpl.UnsafeEnabled {
		mi := &file_proto_data_data_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Mutation) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Mutation) ProtoMessage() {}

func (x *Mutation) ProtoReflect() protoreflect.Message {
	mi := &file_proto_data_data_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Mutation.ProtoReflect.Descriptor instead.
func (*Mutation) Descriptor() ([]byte, []int) {
	return file_proto_data_data_proto_rawDescGZIP(), []int{1}
}

func (x *Mutation) GetKey() []byte {
	if x != nil {
		return x.Key
	}
	return nil
}

func (x *Mutation) GetValue() []byte {
	if x != nil {
		return x.Value
	}
	return nil
}

func (x *Mutation) GetTombstone() *Tombstone {
	if x != nil {
		return x.Tombstone
	}
	return nil
}

type Tombstone struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	DeletionTime *timestamppb.Timestamp `protobuf:"bytes,1,opt,name=DeletionTime,proto3" json:"DeletionTime,omitempty"`
}

func (x *Tombstone) Reset() {
	*x = Tombstone{}
	if protoimpl.UnsafeEnabled {
		mi := &file_proto_data_data_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Tombstone) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Tombstone) ProtoMessage() {}

func (x *Tombstone) ProtoReflect() protoreflect.Message {
	mi := &file_proto_data_data_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Tombstone.ProtoReflect.Descriptor instead.
func (*Tombstone) Descriptor() ([]byte, []int) {
	return file_proto_data_data_proto_rawDescGZIP(), []int{2}
}

func (x *Tombstone) GetDeletionTime() *timestamppb.Timestamp {
	if x != nil {
		return x.DeletionTime
	}
	return nil
}

type IndexEntry struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Key      []byte `protobuf:"bytes,1,opt,name=Key,proto3" json:"Key,omitempty"`
	Position uint64 `protobuf:"varint,2,opt,name=Position,proto3" json:"Position,omitempty"`
}

func (x *IndexEntry) Reset() {
	*x = IndexEntry{}
	if protoimpl.UnsafeEnabled {
		mi := &file_proto_data_data_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *IndexEntry) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*IndexEntry) ProtoMessage() {}

func (x *IndexEntry) ProtoReflect() protoreflect.Message {
	mi := &file_proto_data_data_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use IndexEntry.ProtoReflect.Descriptor instead.
func (*IndexEntry) Descriptor() ([]byte, []int) {
	return file_proto_data_data_proto_rawDescGZIP(), []int{3}
}

func (x *IndexEntry) GetKey() []byte {
	if x != nil {
		return x.Key
	}
	return nil
}

func (x *IndexEntry) GetPosition() uint64 {
	if x != nil {
		return x.Position
	}
	return 0
}

var File_proto_data_data_proto protoreflect.FileDescriptor

var file_proto_data_data_proto_rawDesc = []byte{
	0x0a, 0x15, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2f, 0x64, 0x61, 0x74, 0x61, 0x2f, 0x64, 0x61, 0x74,
	0x61, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x07, 0x6f, 0x69, 0x2e, 0x64, 0x61, 0x74, 0x61,
	0x1a, 0x1f, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75,
	0x66, 0x2f, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x2e, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x22, 0x5d, 0x0a, 0x06, 0x52, 0x65, 0x63, 0x6f, 0x72, 0x64, 0x12, 0x10, 0x0a, 0x03, 0x4c,
	0x53, 0x4e, 0x18, 0x01, 0x20, 0x01, 0x28, 0x04, 0x52, 0x03, 0x4c, 0x53, 0x4e, 0x12, 0x25, 0x0a,
	0x04, 0x44, 0x61, 0x74, 0x61, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x11, 0x2e, 0x6f, 0x69,
	0x2e, 0x64, 0x61, 0x74, 0x61, 0x2e, 0x4d, 0x75, 0x74, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x52, 0x04,
	0x44, 0x61, 0x74, 0x61, 0x12, 0x1a, 0x0a, 0x08, 0x43, 0x68, 0x65, 0x63, 0x6b, 0x73, 0x75, 0x6d,
	0x18, 0x03, 0x20, 0x01, 0x28, 0x0d, 0x52, 0x08, 0x43, 0x68, 0x65, 0x63, 0x6b, 0x73, 0x75, 0x6d,
	0x22, 0x64, 0x0a, 0x08, 0x4d, 0x75, 0x74, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x12, 0x10, 0x0a, 0x03,
	0x4b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x03, 0x4b, 0x65, 0x79, 0x12, 0x14,
	0x0a, 0x05, 0x56, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x05, 0x56,
	0x61, 0x6c, 0x75, 0x65, 0x12, 0x30, 0x0a, 0x09, 0x54, 0x6f, 0x6d, 0x62, 0x73, 0x74, 0x6f, 0x6e,
	0x65, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x12, 0x2e, 0x6f, 0x69, 0x2e, 0x64, 0x61, 0x74,
	0x61, 0x2e, 0x54, 0x6f, 0x6d, 0x62, 0x73, 0x74, 0x6f, 0x6e, 0x65, 0x52, 0x09, 0x54, 0x6f, 0x6d,
	0x62, 0x73, 0x74, 0x6f, 0x6e, 0x65, 0x22, 0x4b, 0x0a, 0x09, 0x54, 0x6f, 0x6d, 0x62, 0x73, 0x74,
	0x6f, 0x6e, 0x65, 0x12, 0x3e, 0x0a, 0x0c, 0x44, 0x65, 0x6c, 0x65, 0x74, 0x69, 0x6f, 0x6e, 0x54,
	0x69, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1a, 0x2e, 0x67, 0x6f, 0x6f, 0x67,
	0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x54, 0x69, 0x6d, 0x65,
	0x73, 0x74, 0x61, 0x6d, 0x70, 0x52, 0x0c, 0x44, 0x65, 0x6c, 0x65, 0x74, 0x69, 0x6f, 0x6e, 0x54,
	0x69, 0x6d, 0x65, 0x22, 0x3a, 0x0a, 0x0a, 0x49, 0x6e, 0x64, 0x65, 0x78, 0x45, 0x6e, 0x74, 0x72,
	0x79, 0x12, 0x10, 0x0a, 0x03, 0x4b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x03,
	0x4b, 0x65, 0x79, 0x12, 0x1a, 0x0a, 0x08, 0x50, 0x6f, 0x73, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x18,
	0x02, 0x20, 0x01, 0x28, 0x04, 0x52, 0x08, 0x50, 0x6f, 0x73, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x42,
	0x10, 0x5a, 0x0e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2d, 0x67, 0x65, 0x6e, 0x2f, 0x64, 0x61, 0x74,
	0x61, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_proto_data_data_proto_rawDescOnce sync.Once
	file_proto_data_data_proto_rawDescData = file_proto_data_data_proto_rawDesc
)

func file_proto_data_data_proto_rawDescGZIP() []byte {
	file_proto_data_data_proto_rawDescOnce.Do(func() {
		file_proto_data_data_proto_rawDescData = protoimpl.X.CompressGZIP(file_proto_data_data_proto_rawDescData)
	})
	return file_proto_data_data_proto_rawDescData
}

var file_proto_data_data_proto_msgTypes = make([]protoimpl.MessageInfo, 4)
var file_proto_data_data_proto_goTypes = []interface{}{
	(*Record)(nil),                // 0: oi.data.Record
	(*Mutation)(nil),              // 1: oi.data.Mutation
	(*Tombstone)(nil),             // 2: oi.data.Tombstone
	(*IndexEntry)(nil),            // 3: oi.data.IndexEntry
	(*timestamppb.Timestamp)(nil), // 4: google.protobuf.Timestamp
}
var file_proto_data_data_proto_depIdxs = []int32{
	1, // 0: oi.data.Record.Data:type_name -> oi.data.Mutation
	2, // 1: oi.data.Mutation.Tombstone:type_name -> oi.data.Tombstone
	4, // 2: oi.data.Tombstone.DeletionTime:type_name -> google.protobuf.Timestamp
	3, // [3:3] is the sub-list for method output_type
	3, // [3:3] is the sub-list for method input_type
	3, // [3:3] is the sub-list for extension type_name
	3, // [3:3] is the sub-list for extension extendee
	0, // [0:3] is the sub-list for field type_name
}

func init() { file_proto_data_data_proto_init() }
func file_proto_data_data_proto_init() {
	if File_proto_data_data_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_proto_data_data_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Record); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_proto_data_data_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Mutation); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_proto_data_data_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Tombstone); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_proto_data_data_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*IndexEntry); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_proto_data_data_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   4,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_proto_data_data_proto_goTypes,
		DependencyIndexes: file_proto_data_data_proto_depIdxs,
		MessageInfos:      file_proto_data_data_proto_msgTypes,
	}.Build()
	File_proto_data_data_proto = out.File
	file_proto_data_data_proto_rawDesc = nil
	file_proto_data_data_proto_goTypes = nil
	file_proto_data_data_proto_depIdxs = nil
}
